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

import binascii
import json
import logging
import pytest
import sqlalchemy
import string

from stage.utils.utils_primary_key_metadata import PRIMARY_KEY_NON_NUMERIC_METADATA_SQLSERVER, \
    PRIMARY_KEY_NUMERIC_METADATA_SQLSERVER, get_create_table_query_non_numeric, get_create_table_query_numeric, \
    get_insert_query_non_numeric, get_insert_query_numeric
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string
from time import sleep, time

logger = logging.getLogger(__name__)

DEFAULT_SCHEMA_NAME = 'dbo'

BASIC_RECORDS = 'BASIC'
DISCARD_BEFORE_UPDATE_RECORDS = 'DISCARD_BEFORE_UPDATE'
RICH_RECORDS = 'RICH'

BASIC_RECORDS_CODE = '1'
DISCARD_BEFORE_UPDATE_RECORDS_CODE = '2'
RICH_RECORDS_CODE = '3'


def setup_table(connection, schema_name, table_name, sample_data=None, capture_instance_name=None):
    """Create table and insert the sample data into the table"""
    table = create_table(connection, schema_name, table_name)

    if capture_instance_name is None:
        capture_instance_name = f'{schema_name}_{table_name}'

    logger.info('Enabling CDC on %s.%s...', schema_name, table_name)
    connection.execute(f'exec sys.sp_cdc_enable_table @source_schema=\'{schema_name}\', '
                       f'@source_name=\'{table_name}\', '
                       f'@capture_instance=\'{capture_instance_name}\', '
                       f'@supports_net_changes=1, @role_name=NULL')

    _wait_until_is_tracked_by_cdc(connection, table_name, 1)

    if sample_data is not None:
        add_data_to_table(connection, table, sample_data)

    return table


def add_data_to_table(connection, table, sample_data):
    logger.info('Adding %s rows into %s...', len(sample_data), table)
    connection.execute(table.insert(), sample_data)


def create_table(database, schema_name, table_name):
    """Create table with the folloiwng scheam: id int primary key, name varchar(25), dt datetime"""
    logger.info('Creating table %s.%s...', schema_name, table_name)
    table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                             sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, autoincrement=False),
                             sqlalchemy.Column('name', sqlalchemy.String(25)),
                             sqlalchemy.Column('dt', sqlalchemy.String(25)),
                             schema=schema_name)

    table.create(database.engine)

    return table


def assert_table_replicated(database, sample_data, schema_name, table_name):
    """Assert the sample data matches with the desitnation table data wrote using JDBC Producer"""
    db_engine = database.engine

    target_table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                    autoload=True, autoload_with=db_engine,
                                    schema=schema_name)
    target_result = db_engine.execute(target_table.select().order_by(target_table.c['id']))
    target_result_list = target_result.fetchall()
    target_result.close()

    input_values = [tuple(data.values()) for data in sample_data]

    assert sorted(input_values) == sorted(target_result_list)


def assert_recovered_data(initial_data, output_records, expected_number_of_records, record_format=BASIC_RECORDS):
    assert len(output_records) == expected_number_of_records

    recovered_information = []
    if record_format == RICH_RECORDS:
        for record in output_records:
            assert 'Data' in record.field
            assert record.field['Data'] is not None
            recovered_information.append({
                'id': record.field['Data']['id'],
                'name': record.field['Data']['name'],
                'dt': record.field['Data']['dt']
            })
    else:
        for record in output_records:
            recovered_information.append(record.field)

    assert initial_data.sort(key=lambda x: x['id']) == recovered_information.sort(key=lambda x: x['id'].value)


def setup_sample_data(no_of_records):
    """Generate the given number of sample data with 'id', 'name', and 'dt'"""
    rows_in_database = [{'id': counter, 'name': get_random_string(string.ascii_lowercase, 20), 'dt': '2017-05-03'}
                        for counter in range(0, no_of_records)]
    return rows_in_database


def wait_for_data_in_ct_table(ct_table_name, no_of_records, database=None, timeout_sec=50):
    """Wait for data is captured by CDC jobs in SQL Server
    (i.e, number of records in CT table is equal to the total number of records).
    """
    logger.info('Waiting for no_data_event to be updated in %s seconds ...', timeout_sec)
    start_waiting_time = time()
    stop_waiting_time = start_waiting_time + timeout_sec
    db_engine = database.engine

    while time() < stop_waiting_time:
        event_table = sqlalchemy.Table(ct_table_name, sqlalchemy.MetaData(), autoload=True,
                                       autoload_with=db_engine, schema='cdc')
        event_result = db_engine.execute(event_table.select())
        event_result_list = event_result.fetchall()
        event_result.close()

        if len(event_result_list) >= no_of_records:
            logger.info('%s of data is captured in CT Table %s', no_of_records, ct_table_name)
            return
        sleep(5)

    raise Exception('Timed out after %s seconds while waiting for captured data.', timeout_sec)


@database('sqlserver')
@pytest.mark.parametrize('no_of_threads', [1, 5])
@sdc_min_version('3.0.1.0')
def test_sql_server_cdc_with_specific_capture_instance_name(
        sdc_builder,
        sdc_executor,
        database,
        no_of_threads
):
    """Test for SQL Server CDC origin stage when capture instance is configured.
    We do so by capturing Insert Operation on CDC enabled table
    using SQL Server CDC Origin and having a pipeline which reads that data using SQL Server CDC origin stage.
    We set up no_of_tables of CDC tables in SQL Server and configured one specific capture instance name
    so that the records from one table will be stored in SQL Server table using JDBC Producer.
    Data is then asserted for what is captured at SQL Server Job and what we read in the pipeline.

    The pipeline looks like:
        sql_server_cdc_origin >> wiretap
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    try:
        connection = database.engine.connect()
        schema_name = DEFAULT_SCHEMA_NAME
        tables = []
        table_configs = []
        no_of_records = 5
        target_table_index = 2
        rows_in_database = setup_sample_data(no_of_threads * no_of_records)

        # setup the tables first
        for index in range(0, no_of_threads):
            table_name = get_random_string(string.ascii_lowercase, 20)
            # split the rows_in_database into no_of_records for each table
            # e.g. for no_of_records=5, the first table inserts rows_in_database[0:5]
            # and the second table inserts rows_in_database[5:10]
            table = setup_table(connection, schema_name, table_name,
                                rows_in_database[(index * no_of_records): ((index + 1) * no_of_records)])
            tables.append(table)
            table_configs.append({'capture_instance': f'{schema_name}_{table_name}'})

        rows_in_database[target_table_index * no_of_records: (target_table_index + 1) * no_of_records]

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(maximum_pool_size=no_of_threads,
                                      number_of_threads=no_of_threads,
                                      table_configs=table_configs)

        wiretap = pipeline_builder.add_wiretap()
        sql_server_cdc >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # wait for data captured by cdc jobs in sql server before starting the pipeline
        for table_config in table_configs:
            ct_table_name = f'{table_config.get("capture_instance")}_CT'
            wait_for_data_in_ct_table(ct_table_name, no_of_records, database, timeout_sec=120)

        expected_no_of_records = no_of_records * no_of_threads
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(expected_no_of_records)
        sdc_executor.stop_pipeline(pipeline)

        assert_recovered_data(rows_in_database, wiretap.output_records, expected_no_of_records)

    finally:
        for table in tables:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)


@database('sqlserver')
@sdc_min_version('3.6.0')
@pytest.mark.parametrize('use_table', [True, False])
def test_sql_server_cdc_with_empty_initial_offset(
        sdc_builder,
        sdc_executor,
        database,
        use_table
):
    """Test for SQL Server CDC origin stage with the empty initial offset (fetch all changes)
    on both use table config is true and false

    The pipeline looks like:
        sql_server_cdc_origin >> wiretap
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    try:
        connection = database.engine.connect()
        schema_name = DEFAULT_SCHEMA_NAME
        no_of_records = 10
        rows_in_database = setup_sample_data(no_of_records)

        # create the table with the above sample data
        table_name = get_random_string(string.ascii_lowercase, 20)
        table = setup_table(connection, schema_name, table_name, rows_in_database)

        # get the capture_instance_name
        capture_instance_name = f'{schema_name}_{table_name}'

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(table_configs=[{'capture_instance': capture_instance_name}],
                                      use_direct_table_query=use_table)

        wiretap = pipeline_builder.add_wiretap()
        sql_server_cdc >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(no_of_records)
        sdc_executor.stop_pipeline(pipeline)

        assert_recovered_data(rows_in_database, wiretap.output_records, no_of_records)

    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)


@database('sqlserver')
@sdc_min_version('3.6.0')
@pytest.mark.parametrize('use_table', [True, False])
def test_sql_server_cdc_with_nonempty_initial_offset(
        sdc_builder,
        sdc_executor,
        database,
        use_table
):
    """Test for SQL Server CDC origin stage with non-empty initial offset (fetch the data from the given LSN)
    on both use table config is true and false

    The pipeline looks like:
        sql_server_cdc_origin >> wiretap
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    try:
        connection = database.engine.connect()
        schema_name = DEFAULT_SCHEMA_NAME
        first_no_of_records = 3
        second_no_of_records = 8
        total_no_of_records = first_no_of_records + second_no_of_records
        rows_in_database = setup_sample_data(total_no_of_records)

        # create the table and insert the first half of the rows
        table_name = get_random_string(string.ascii_lowercase, 20)
        capture_instance_name = f'{schema_name}_{table_name}'
        table = setup_table(connection, schema_name, table_name, rows_in_database[0:first_no_of_records])
        ct_table_name = f'{capture_instance_name}_CT'
        wait_for_data_in_ct_table(ct_table_name, first_no_of_records, database)

        # insert the last half of the sample data
        add_data_to_table(connection, table, rows_in_database[first_no_of_records:total_no_of_records])
        wait_for_data_in_ct_table(ct_table_name, total_no_of_records, database)

        # get the capture_instance_name
        capture_instance_name = f'{schema_name}_{table_name}'

        # get the current LSN
        currentLSN = binascii.hexlify(connection.execute('SELECT sys.fn_cdc_get_max_lsn() as currentLSN').scalar())

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(table_configs=[{'capture_instance': capture_instance_name,
                                                      'initialOffset': currentLSN.decode("utf-8")}],
                                      use_direct_table_query=use_table
                                      )

        wiretap = pipeline_builder.add_wiretap()
        sql_server_cdc >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(second_no_of_records)
        sdc_executor.stop_pipeline(pipeline)

        assert_recovered_data(
            rows_in_database[first_no_of_records:total_no_of_records],
            wiretap.output_records,
            second_no_of_records
        )

    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)

        if connection is not None:
            connection.close()


@database('sqlserver')
@sdc_min_version('3.6.0')
@pytest.mark.parametrize('use_table', [True, False])
@pytest.mark.timeout(180)
def test_sql_server_cdc_insert_and_update_basic_record_format(sdc_builder, sdc_executor, database, use_table):
    """Test for SQL Server CDC origin stage with insert and update ops

    The pipeline looks like:
        sql_server_cdc_origin >> jdbc_producer
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    try:
        connection = database.engine.connect()
        schema_name = DEFAULT_SCHEMA_NAME

        rows_in_database = setup_sample_data(1)

        # create the table and insert 1 row
        table_name = get_random_string(string.ascii_lowercase, 20)
        table = setup_table(connection, schema_name, table_name, rows_in_database)

        # update the row
        updated_name = 'jisun'
        connection.execute(table.update()
                           .where(table.c.id == 0)
                           .values(name=updated_name))

        total_no_of_records = 3

        # get the capture_instance_name
        capture_instance_name = f'{schema_name}_{table_name}'

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(table_configs=[{'capture_instance': capture_instance_name}],
                                      use_direct_table_query=use_table,
                                      fetch_size=2
                                      )

        # create the destination table
        dest_table_name = get_random_string(string.ascii_uppercase, 9)
        dest_table = create_table(database, DEFAULT_SCHEMA_NAME, dest_table_name)

        jdbc_producer = pipeline_builder.add_stage('JDBC Producer')

        jdbc_producer.set_attributes(schema_name=DEFAULT_SCHEMA_NAME,
                                     table_name=dest_table_name,
                                     default_operation='INSERT',
                                     field_to_column_mapping=[])

        sql_server_cdc >> jdbc_producer
        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # wait for data captured by cdc jobs in sql server before starting the pipeline
        ct_table_name = f'{capture_instance_name}_CT'
        wait_for_data_in_ct_table(ct_table_name, total_no_of_records, database)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(total_no_of_records)
        sdc_executor.stop_pipeline(pipeline)
        expected_rows_in_database = [
            {'id': rows_in_database[0].get('id'), 'name': updated_name, 'dt': rows_in_database[0].get('dt')}]
        assert_table_replicated(database, expected_rows_in_database, DEFAULT_SCHEMA_NAME, dest_table_name)

    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)

        if dest_table is not None:
            logger.info('Dropping table %s in %s database...', dest_table, database.type)
            dest_table.drop(database.engine)

        if connection is not None:
            connection.close()


@database('sqlserver')
@sdc_min_version('3.6.0')
@pytest.mark.parametrize('use_table', [True, False])
@pytest.mark.timeout(180)
def test_sql_server_cdc_insert_update_delete_basic_record_format(sdc_builder, sdc_executor, database, use_table):
    """Test for SQL Server CDC origin stage with insert, update, and delete ops

    The pipeline looks like:
        sql_server_cdc_origin >> jdbc_producer
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    try:
        connection = database.engine.connect()
        schema_name = DEFAULT_SCHEMA_NAME

        rows_in_database = setup_sample_data(1)

        # create the table and insert 1 row
        table_name = get_random_string(string.ascii_lowercase, 20)
        table = setup_table(connection, schema_name, table_name, rows_in_database)

        # update the row
        updated_name = 'jisun'
        connection.execute(table.update()
                           .where(table.c.id == 0)
                           .values(name=updated_name))

        # delete the rows
        connection.execute(table.delete())

        total_no_of_records = 4

        # get the capture_instance_name
        capture_instance_name = f'{schema_name}_{table_name}'

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(table_configs=[{'capture_instance': capture_instance_name}],
                                      use_direct_table_query=use_table,
                                      fetch_size=1
                                      )

        # create the destination table
        dest_table_name = get_random_string(string.ascii_uppercase, 9)
        dest_table = create_table(database, DEFAULT_SCHEMA_NAME, dest_table_name)

        jdbc_producer = pipeline_builder.add_stage('JDBC Producer')

        jdbc_producer.set_attributes(schema_name=DEFAULT_SCHEMA_NAME,
                                     table_name=dest_table_name,
                                     default_operation='INSERT',
                                     field_to_column_mapping=[])

        sql_server_cdc >> jdbc_producer
        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # wait for data captured by cdc jobs in sql server before starting the pipeline
        ct_table_name = f'{capture_instance_name}_CT'
        wait_for_data_in_ct_table(ct_table_name, total_no_of_records, database)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(total_no_of_records)
        sdc_executor.stop_pipeline(pipeline)
        expected_rows_in_database = []
        assert_table_replicated(database, expected_rows_in_database, DEFAULT_SCHEMA_NAME, dest_table_name)

    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)

        if dest_table is not None:
            logger.info('Dropping table %s in %s database...', dest_table, database.type)
            dest_table.drop(database.engine)

        if connection is not None:
            connection.close()


@database('sqlserver')
@sdc_min_version('3.6.0')
@pytest.mark.parametrize('use_table', [True, False])
@pytest.mark.timeout(180)
def test_sql_server_cdc_multiple_tables(
        sdc_builder,
        sdc_executor,
        database,
        use_table
):
    """Test for SQL Server CDC origin stage with multiple transactions on multiple CDC tables (SDC-10926)

    The pipeline looks like:
        sql_server_cdc_origin >> jdbc_producer
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    try:
        connection = database.engine.connect()
        no_of_tables = 3
        table_configs = []
        tables = []

        rows_in_database = setup_sample_data(3)

        updated_name = 'jisun'
        for index in range(0, no_of_tables):
            # create the table and insert 1 row and update the row
            table_name = get_random_string(string.ascii_lowercase, 20)
            table = setup_table(connection, DEFAULT_SCHEMA_NAME, table_name, rows_in_database[index:index + 1])

            connection.execute(table.update()
                               .values(name=updated_name))

            table_configs.append({'capture_instance': f'{DEFAULT_SCHEMA_NAME}_{table_name}'})
            tables.append(table)

        # update the row from the first table
        new_updated_name = 'sdc'
        connection.execute(tables[0].update().values(name=new_updated_name))

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(fetch_size=1,
                                      table_configs=table_configs,
                                      use_direct_table_query=use_table
                                      )

        wiretap = pipeline_builder.add_wiretap()
        sql_server_cdc >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        total_no_of_records = 11

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(total_no_of_records)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert len(records) == total_no_of_records

        records.sort(key=lambda record: _get_data_field_from_record(record, 'id').value)

        date = '2017-05-03'
        index = 0
        for row_id in range(0, 3):
            data = rows_in_database[row_id]
            _check_record(records[index], '1', row_id, data['name'], data['dt'], False)
            _check_record(records[index + 1], '5', row_id, data['name'], data['dt'], False)
            _check_record(records[index + 2], '3', row_id, updated_name, date, False)
            index += 3

            if row_id == 0:
                _check_record(records[index], '5', row_id, updated_name, date, False)
                _check_record(records[index + 1], '3', row_id, new_updated_name, date, False)
                index += 2

        sqlserver_cdc_pipeline_history_metrics = sdc_executor.get_pipeline_history(pipeline).latest.metrics
        records_sent = sqlserver_cdc_pipeline_history_metrics.counter('pipeline.batchInputRecords.counter').count
        assert records_sent == total_no_of_records
    finally:
        for index in range(0, no_of_tables):
            logger.info('Dropping table %s in %s database...', tables[index], database.type)
            tables[index].drop(database.engine)

        if connection is not None:
            connection.close()


@database('sqlserver')
@pytest.mark.timeout(180)
@pytest.mark.parametrize('record_format', [BASIC_RECORDS, DISCARD_BEFORE_UPDATE_RECORDS, RICH_RECORDS])
def test_sql_server_cdc_source_table_in_record_header(
        sdc_builder,
        sdc_executor,
        database,
        record_format
):
    """Test for SQL Server CDC origin stage puts the source table in a record header attribute,
        * jdbc.cdc.source_schema_name = <source schema>
        * jdbc.cdc.source_name = <source table>

    The pipeline looks like:
        sql_server_cdc_origin >> wiretap
    """
    if Version(sdc_builder.version) < Version('5.2.0') and record_format == RICH_RECORDS:
        pytest.skip(f'The "Combine Update Records" option / "Rich" Record Format in not available until version 5.2.0. '
                    f'Skipping as the current version is {sdc_builder.version}.')
    if Version(sdc_builder.version) < Version('5.6.0') and record_format == DISCARD_BEFORE_UPDATE_RECORDS:
        pytest.skip(f'The record format "Basic Discarding \'Before Update\' Records" was introduced in SDC version '
                    f'5.6.0. Skipping as the current version is {sdc_builder.version}.')

    table = None
    connection = None
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    try:
        connection = database.engine.connect()
        total_no_of_records = 1

        rows_in_database = setup_sample_data(total_no_of_records)

        table_name = get_random_string(string.ascii_lowercase, 20)
        schema_name = get_random_string(string.ascii_lowercase, 3)
        capture_instance_name = get_random_string(string.ascii_lowercase, 20)

        # create schema & table
        connection.execute(f'CREATE SCHEMA {schema_name}')
        table = setup_table(connection, schema_name, table_name,
                            rows_in_database[0:1], capture_instance_name)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(fetch_size=1,
                                      max_batch_size_in_records=1,
                                      table_configs=[{'capture_instance': capture_instance_name}]
                                      )

        if Version(sdc_builder.version) >= Version('5.2.0'):
            if Version(sdc_builder.version) < Version('5.6.0'):
                sql_server_cdc.combine_update_records = (record_format == RICH_RECORDS)
            else:
                sql_server_cdc.record_format = record_format

        wiretap = pipeline_builder.add_wiretap()

        sql_server_cdc >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # wait for data captured by cdc jobs in sql server before capturing the pipeline
        ct_table_name = f'{capture_instance_name}_CT'
        wait_for_data_in_ct_table(ct_table_name, total_no_of_records / 2, database)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', total_no_of_records)
        sdc_executor.stop_pipeline(pipeline)

        # assert all the data captured have the same raw_data
        for record in wiretap.output_records:
            field_data = record.field['Data'] if record_format == RICH_RECORDS else record.field
            assert field_data['id'] == rows_in_database[0].get('id')
            assert field_data['name'] == rows_in_database[0].get('name')
            assert field_data['dt'] == rows_in_database[0].get('dt')
            assert record.header['values']['jdbc.cdc.source_schema_name'] == schema_name
            assert record.header['values']['jdbc.cdc.source_name'] == table_name
    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)

        if schema_name is not None:
            logger.info('Dropping schema %s in %s database...', schema_name, database.type)
            connection.execute(f'drop schema {schema_name}')

        if connection is not None:
            connection.close()


@database('sqlserver')
@sdc_min_version('3.8.0')
@pytest.mark.timeout(180)
def test_sql_server_cdc_starting_without_operation_committed_offset(
        sdc_builder,
        sdc_executor,
        database
):
    """Test for SQL Server CDC origin stage running on missing __$operation in the committed offset
        __$operation field was introduced after 3.8.0

    The pipeline looks like:
        sql_server_cdc_origin >> wiretap
    """
    table = None
    connection = None
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    try:
        connection = database.engine.connect()
        total_no_of_records = 1

        rows_in_database = setup_sample_data(total_no_of_records)

        table_name = get_random_string(string.ascii_lowercase, 20)
        # get the capture_instance_name
        capture_instance_name = f'{DEFAULT_SCHEMA_NAME}_{table_name}'

        # create schema & table
        table = setup_table(connection, DEFAULT_SCHEMA_NAME, table_name, rows_in_database)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(fetch_size=1,
                                      max_batch_size_in_records=1,
                                      table_configs=[{'capture_instance': capture_instance_name}]
                                      )

        wiretap = pipeline_builder.add_wiretap()

        sql_server_cdc >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # We hard code offset to be pre-migration to multi-threaded origin and thus forcing the origin to upgrade it
        offset = {
            'version': 2,
            'offsets': {
                '$com.streamsets.pipeline.stage.origin.jdbc.CDC.sqlserver.SQLServerCDCSource.offset.version$': '1',
                f'tableName=cdc.{capture_instance_name}_CT;;;partitioned=false;;;partitionSequence=-1;;;partitionStartOffsets=;;;partitionMaxOffsets=;;;usingNonIncrementalLoad=false': '__$seqval=0::__$start_lsn=0'
            }
        }

        sdc_executor.api_client.update_pipeline_committed_offsets(pipeline.id, body=offset)

        # wait for data captured by cdc jobs in sql server before capturing the pipeline
        ct_table_name = f'{capture_instance_name}_CT'
        wait_for_data_in_ct_table(ct_table_name, total_no_of_records, database)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', total_no_of_records)
        sdc_executor.stop_pipeline(pipeline)

        # assert all the data captured have the same raw_data
        output_records = wiretap.output_records
        for record in output_records:
            assert record.field['id'] == rows_in_database[0].get('id')
            assert record.field['name'] == rows_in_database[0].get('name')
            assert record.field['dt'] == rows_in_database[0].get('dt')
    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)

        if connection is not None:
            connection.close()


@database('sqlserver')
@sdc_min_version('3.0.0.0')
@pytest.mark.timeout(180)
@pytest.mark.parametrize('record_format', [BASIC_RECORDS, DISCARD_BEFORE_UPDATE_RECORDS, RICH_RECORDS])
def test_schema_change(
        sdc_builder,
        sdc_executor,
        database,
        keep_data,
        record_format
):
    """
    Test for SQL Server CDC origin stage when schema change is enabled. We do so by capturing Insert Operation on CDC
    enabled table(s) using SQL Server CDC Origin and having a pipeline which reads that data using SQL Server CDC origin
    stage. The records in the pipeline are captured with wiretap and its contents are checked to see if the schema
    changes are correctly reflected in the record fields returned.

    The pipeline looks like:
        sql_server_cdc_origin >> wiretap
    """

    if Version(sdc_builder.version) < Version('5.2.0') and record_format == RICH_RECORDS:
        pytest.skip(f'The "Combine Update Records" / "Rich" Record Format is not available in versions prior to 5.2.0. '
                    f'Skipping as the current version is {sdc_builder.version}.')

    if Version(sdc_builder.version) < Version('5.6.0') and record_format == DISCARD_BEFORE_UPDATE_RECORDS:
        pytest.skip(f'The record format "Basic Discarding \'Before Update\' Records" is not available in versions prior'
                    f' to 5.6.0. Skipping as the current version is {sdc_builder.version}.')

    num_of_tables = 2
    schema_name = DEFAULT_SCHEMA_NAME
    table_prefix = get_random_string(string.ascii_lowercase, 10)

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('SQL Server CDC Client')
    origin.allow_late_tables = True
    origin.enable_schema_changes_event = True
    # when allow_late_tables = true, the pipeline runs one background thread
    # to spool the list of cdc tables
    origin.maximum_pool_size = num_of_tables + 1
    origin.minimum_idle_connections = num_of_tables + 1
    origin.new_table_discovery_interval = '${1 * SECONDS}'
    origin.table_configs = [{'capture_instance': f'dbo_{table_prefix}_%'}]
    origin.number_of_threads = num_of_tables

    if Version('5.2.0') <= Version(sdc_builder.version):
        if Version(sdc_builder.version) < Version('5.6.0'):
            origin.combine_update_records = (record_format == RICH_RECORDS)
        else:
            origin.record_format = record_format

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    connection = database.engine.connect()
    try:
        global_index = 0
        tables = []

        for index in range(0, num_of_tables):
            table_name = f"{table_prefix}_{get_random_string(string.ascii_lowercase, 20)}"
            connection.execute(f"CREATE TABLE {schema_name}.{table_name}(id INT)")
            _enable_cdc(connection, schema_name, table_name)
            connection.execute(f"INSERT INTO {schema_name}.{table_name} VALUES({global_index})")
            global_index += 1

            tables.append(table_name)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', num_of_tables)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert len(records) == num_of_tables
        using_rich_format = record_format == RICH_RECORDS
        records.sort(key=_sort_combined_records if using_rich_format else _sort_records)
        for i in range(0, num_of_tables):
            assert _get_data_field_from_record(records[i], 'id', record_format) == i

        # Add a new column to the tables
        for table_name in tables:
            logger.info('Adding the column new_column varchar(10) on %s.%s...', schema_name, table_name)
            connection.execute(f'ALTER TABLE {table_name} ADD new_column INT')

            _disable_cdc(connection, schema_name, table_name)
            _enable_cdc(connection, schema_name, table_name, f"{schema_name}_{table_name}_2")

        # Reset record accumulation
        wiretap.reset()

        # Insert new data to the tables
        for table_name in tables:
            logger.info("Inserting new row into %s", table_name)
            connection.execute(f"INSERT INTO {schema_name}.{table_name} VALUES({global_index}, {global_index})")
            global_index += 1

        logger.info("Waiting on pipeline to finish processing all records")
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', num_of_tables)
        sdc_executor.stop_pipeline(pipeline)

        # All data were read
        records = wiretap.output_records
        assert len(records) == num_of_tables
        records.sort(key=_sort_combined_records if using_rich_format else _sort_records)
        for i in range(0, num_of_tables):
            record_field_data = records[i].field['Data'] if using_rich_format else records[i].field
            assert record_field_data['id'] == num_of_tables + i
            assert record_field_data['new_column'] == num_of_tables + i
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        if not keep_data:
            for table in tables:
                logger.info('Dropping table %s in %s database...', table, database.type)
                connection.execute(f"DROP TABLE {schema_name}.{table}")


@database('sqlserver')
@sdc_min_version('5.2.0')
@pytest.mark.parametrize('use_table', [True, False])
@pytest.mark.timeout(180)
def test_combined_update_record_format(sdc_builder, sdc_executor, database, use_table):
    """
    Tests the format of the data returned in insert, update and delete records for the SQL Server CDC origin stage when
    the "Combine Update Records" option is activated.

    As of SDC version 5.6.0, activating the "Combine Update Records" option is equivalent to selecting the "Rich"
    Record Format.

    The pipeline looks like:
        sql_server_cdc_origin >> jdbc_producer
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    table = None
    pipeline = None
    connection = database.engine.connect()

    try:
        # Create a table, insert 1 row, update it and then delete it
        table_name = get_random_string(string.ascii_lowercase, 20)
        rows_in_database = setup_sample_data(1)
        table = setup_table(connection, DEFAULT_SCHEMA_NAME, table_name, rows_in_database)
        updated_name = 'new_updated_name'
        connection.execute(table.update().where(table.c.id == 0).values(name=updated_name))
        connection.execute(table.delete())

        total_number_of_records = 3
        capture_instance_name = f'{DEFAULT_SCHEMA_NAME}_{table_name}'

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(
            table_configs=[{'capture_instance': capture_instance_name}],
            use_direct_table_query=use_table,
            fetch_size=1
        )

        if Version(sdc_builder.version) < Version('5.6.0'):
            sql_server_cdc.combine_update_records = True
        else:
            sql_server_cdc.record_format = RICH_RECORDS

        wiretap = pipeline_builder.add_wiretap()
        sql_server_cdc >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Wait for the data to be captured by CDC jobs in SQL Server before starting the pipeline
        ct_table_name = f'{capture_instance_name}_CT'
        wait_for_data_in_ct_table(ct_table_name, total_number_of_records, database)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(total_number_of_records)

        assert len(wiretap.output_records) == total_number_of_records

        old_row = rows_in_database[0]
        new_row = rows_in_database[0]
        new_row['name'] = updated_name

        for record in wiretap.output_records:
            operation = record.header.values['sdc.operation.type']
            if operation == '1':
                assert 'OldData' not in record.field
                assert 'Data' in record.field
                assert record.field['Data'] is not None
                assert sorted(record.field['Data']) == sorted(old_row)

            elif operation == '2':
                assert 'OldData' in record.field
                assert record.field['OldData'] is not None
                assert 'Data' not in record.field
                assert sorted(record.field['OldData']) == sorted(new_row)

            elif operation == '3':
                assert 'OldData' in record.field
                assert record.field['OldData'] is not None
                assert sorted(record.field['OldData']) == sorted(old_row)

                assert 'Data' in record.field
                assert record.field['Data'] is not None
                assert sorted(record.field['Data']) == sorted(new_row)

            else:
                assert False, 'Should not reach here. An unexpected value has been set for "sdc.operation.type".'

        sdc_executor.stop_pipeline(pipeline)

    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)

        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)

        if connection is not None:
            connection.close()


@database('sqlserver')
@sdc_min_version('5.6.0')
@pytest.mark.parametrize('use_table', [True, False])
@pytest.mark.timeout(180)
def test_discard_before_update_record_format(sdc_builder, sdc_executor, database, use_table):
    """
    Tests the format of the data returned in insert, update and delete records for the SQL Server CDC origin stage when
    the "Basic Discarding 'Before Update' Records" record format is defined.

    The pipeline looks like:
        sql_server_cdc_origin >> wiretap
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    table = None
    pipeline = None
    connection = database.engine.connect()

    try:
        # Create a table, insert 1 row, update it and then delete it
        table_name = get_random_string(string.ascii_lowercase, 20)
        rows_in_database = setup_sample_data(1)
        table = setup_table(connection, DEFAULT_SCHEMA_NAME, table_name, rows_in_database)
        updated_name = 'new_updated_name'
        connection.execute(table.update().where(table.c.id == 0).values(name=updated_name))
        connection.execute(table.delete())

        total_number_of_records = 3
        capture_instance_name = f'{DEFAULT_SCHEMA_NAME}_{table_name}'

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(
            table_configs=[{'capture_instance': capture_instance_name}],
            use_direct_table_query=use_table,
            fetch_size=1,
            record_format=DISCARD_BEFORE_UPDATE_RECORDS
        )

        wiretap = pipeline_builder.add_wiretap()
        sql_server_cdc >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Wait for the data to be captured by CDC jobs in SQL Server before starting the pipeline
        ct_table_name = f'{capture_instance_name}_CT'
        wait_for_data_in_ct_table(ct_table_name, total_number_of_records, database)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(total_number_of_records)

        output_records = wiretap.output_records
        assert len(output_records) == total_number_of_records

        old_row = rows_in_database[0]
        new_row = rows_in_database[0]
        new_row['name'] = updated_name

        for record in output_records:
            operation = record.header.values['sdc.operation.type']
            assert 'OldData' not in record.field
            assert 'Data' not in record.field
            if operation == '1':
                assert sorted(record.field) == sorted(old_row)
            elif operation == '2' or operation == '3':
                assert sorted(record.field) == sorted(new_row)
            else:
                assert False, 'Should not reach here. An unexpected value has been set for "sdc.operation.type".'

        sdc_executor.stop_pipeline(pipeline)

    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)

        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)

        if connection is not None:
            connection.close()


@sdc_min_version('5.2.0')
@database('sqlserver')
@pytest.mark.parametrize('use_table', [True, False])
@pytest.mark.parametrize('record_format', [BASIC_RECORDS, DISCARD_BEFORE_UPDATE_RECORDS, RICH_RECORDS])
def test_primary_keys_headers(sdc_builder, sdc_executor, database, use_table, record_format):
    """
    Test to check the primary keys are present in the headers of the output records.

    SQL Server does not consider an update as such if the primary keys are changed (in such case it is considered a
    delete + an insert), so the before and after values will always match.
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    if (record_format == BASIC_RECORDS) and (Version(sdc_builder.version) < Version('5.5.0')):
        pytest.skip(f'The primary keys header values are not generated if not using the "Combine Update Records" format'
                    f' in versions prior to 5.5.0. Skipping as the current version is {sdc_builder.version}.')

    if (record_format == DISCARD_BEFORE_UPDATE_RECORDS) and (Version(sdc_builder.version) < Version('5.6.0')):
        pytest.skip(f'The record format "Basic Discarding \'Before Update\' Records" was introduced in SDC version '
                    f'5.6.0. Skipping as the current version is {sdc_builder.version}.')

    pipeline = None
    table_name = get_random_string(string.ascii_lowercase, 20)

    try:
        # Create the table
        connection = database.engine.connect()

        logger.info('Creating source table %s in %s database ...', table_name, database.type)
        table = sqlalchemy.Table(
            table_name,
            sqlalchemy.MetaData(database.engine),
            sqlalchemy.Column('name', sqlalchemy.String(64), primary_key=True),
            sqlalchemy.Column('pokedex_id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('type', sqlalchemy.String(64)),
            sqlalchemy.Column('generation', sqlalchemy.Integer)
        )

        table.create(database.engine)
        capture_instance_name = f'{DEFAULT_SCHEMA_NAME}_{table_name}'
        _enable_cdc(connection, DEFAULT_SCHEMA_NAME, table_name, capture_instance_name)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(
            table_configs=[{'capture_instance': capture_instance_name}],
            use_direct_table_query=use_table
        )

        if Version(sdc_builder.version) < Version('5.6.0'):
            sql_server_cdc.combine_update_records = (record_format == RICH_RECORDS)
        else:
            sql_server_cdc.record_format = record_format

        wiretap = pipeline_builder.add_wiretap()
        sql_server_cdc >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Define the data for each statement
        initial_data = {'name': 'Azurill', 'pokedex_id': 298, 'type': 'Normal', 'generation': 3}
        updated_data = {'name': 'Azurill', 'pokedex_id': 298, 'type': 'Normal/Fairy', 'generation': 6}

        # Insert some data and update it
        connection.execute(f"""
            insert into {table_name}
            values (
                '{initial_data.get("name")}',
                {initial_data.get("pokedex_id")},
                '{initial_data.get("type")}',
                {initial_data.get("generation")}
            )
        """)

        connection.execute(f"""
            update {table_name}
            set type = '{updated_data.get("type")}', generation = {updated_data.get("generation")}
            where name = '{updated_data.get("name")}' and pokedex_id = {updated_data.get("pokedex_id")}
        """)

        connection.execute(f"delete from {table_name}")

        num_expected_records = 4 if record_format == BASIC_RECORDS else 3

        sdc_executor.start_pipeline(pipeline)
        ct_table_name = f'{capture_instance_name}_CT'
        wait_for_data_in_ct_table(ct_table_name, num_expected_records, database)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', num_expected_records)
        assert len(wiretap.output_records) == num_expected_records

        primary_key_before_prefix = "jdbc.primaryKey.before."
        primary_key_after_prefix = "jdbc.primaryKey.after."
        op_type = 'sdc.operation.type'

        for index in range(0, num_expected_records):
            header_values = wiretap.output_records[index].header.values

            assert primary_key_before_prefix + "type" not in header_values
            assert primary_key_before_prefix + "generation" not in header_values
            assert primary_key_after_prefix + "type" not in header_values
            assert primary_key_after_prefix + "generation" not in header_values

            if index == 1 or (index == 2 and record_format == BASIC_RECORDS):
                assert header_values[op_type] == '3' if (record_format != BASIC_RECORDS or index == 2) else '5'

                assert primary_key_before_prefix + "name" in header_values
                assert primary_key_before_prefix + "pokedex_id" in header_values
                assert primary_key_after_prefix + "name" in header_values
                assert primary_key_after_prefix + "pokedex_id" in header_values

                assert header_values[primary_key_before_prefix + "name"] is not None
                assert header_values[primary_key_before_prefix + "pokedex_id"] is not None
                assert header_values[primary_key_after_prefix + "name"] is not None
                assert header_values[primary_key_after_prefix + "pokedex_id"] is not None

                assert header_values[f"{primary_key_before_prefix}name"] == initial_data.get("name")
                assert header_values[f"{primary_key_before_prefix}pokedex_id"] == f'{initial_data.get("pokedex_id")}'
                assert header_values[f"{primary_key_after_prefix}name"] == updated_data.get("name")
                assert header_values[f"{primary_key_after_prefix}pokedex_id"] == f'{updated_data.get("pokedex_id")}'
            else:
                if index == 0:
                    assert header_values[op_type] == '1'
                else:
                    assert header_values[op_type] == '2'

                assert primary_key_before_prefix + "name" not in header_values
                assert primary_key_before_prefix + "pokedex_id" not in header_values
                assert primary_key_after_prefix + "name" not in header_values
                assert primary_key_after_prefix + "pokedex_id" not in header_values

        sdc_executor.stop_pipeline(pipeline)

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f'drop table if exists {table_name}')

        if pipeline and (sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING'):
            sdc_executor.stop_pipeline(pipeline)


@sdc_min_version('5.2.0')
@database('sqlserver')
@pytest.mark.parametrize('values', ['numeric', 'non-numeric'])
@pytest.mark.parametrize('use_table', [True, False])
@pytest.mark.parametrize('record_format', [BASIC_RECORDS, DISCARD_BEFORE_UPDATE_RECORDS, RICH_RECORDS])
def test_primary_keys_metadata(sdc_builder, sdc_executor, database, use_table, values, record_format):
    """
    Test to check the metadata of the primary keys is correctly set in the headers of the output records.
    """
    if (record_format == BASIC_RECORDS) and (Version(sdc_builder.version) < Version('5.5.0')):
        pytest.skip(f'The primary keys metadata is not generated if not using the "Combine Update Records" format '
                    f'in versions prior to 5.5.0. Skipping as the current version is {sdc_builder.version}.')

    if (record_format == DISCARD_BEFORE_UPDATE_RECORDS) and (Version(sdc_builder.version) < Version('5.6.0')):
        pytest.skip(f'The record format "Basic Discarding \'Before Update\' Records" was introduced in SDC version '
                    f'5.6.0. Skipping as the current version is {sdc_builder.version}.')

    pipeline = None
    table_name = get_random_string(string.ascii_lowercase, 20)

    try:
        connection = database.engine.connect()
        capture_instance_name = f'{DEFAULT_SCHEMA_NAME}_{table_name}'

        if values == 'numeric':
            connection.execute(get_create_table_query_numeric(table_name, database))
            _enable_cdc(connection, DEFAULT_SCHEMA_NAME, table_name, capture_instance_name)
            connection.execute(get_insert_query_numeric(table_name, database))
            primary_key_specification_expected = PRIMARY_KEY_NUMERIC_METADATA_SQLSERVER
        else:
            connection.execute(get_create_table_query_non_numeric(table_name, database))
            _enable_cdc(connection, DEFAULT_SCHEMA_NAME, table_name, capture_instance_name)
            connection.execute(get_insert_query_non_numeric(table_name, database))
            primary_key_specification_expected = PRIMARY_KEY_NON_NUMERIC_METADATA_SQLSERVER

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(
            table_configs=[{'capture_instance': capture_instance_name}],
            use_direct_table_query=use_table,
            fetch_size=1
        )

        if Version(sdc_builder.version) < Version('5.6.0'):
            sql_server_cdc.combine_update_records = (record_format == RICH_RECORDS)
        else:
            sql_server_cdc.record_format = record_format

        wiretap = pipeline_builder.add_wiretap()
        sql_server_cdc >> wiretap.destination

        pipeline = pipeline_builder.build("SQL Server CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)

        assert len(wiretap.output_records) == 1

        record = wiretap.output_records[0]
        assert "jdbc.primaryKeySpecification" in record.header.values
        assert record.header.values["jdbc.primaryKeySpecification"] is not None

        primary_key_specification_json = json.dumps(
            json.loads(record.header.values["jdbc.primaryKeySpecification"]),
            sort_keys=True
        )

        primary_key_specification_expected_json = json.dumps(
            json.loads(primary_key_specification_expected),
            sort_keys=True
        )

        assert primary_key_specification_json == primary_key_specification_expected_json

        sdc_executor.stop_pipeline(pipeline)

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f'drop table if exists {table_name}')

        if pipeline and (sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING'):
            sdc_executor.stop_pipeline(pipeline)


@database('sqlserver')
@sdc_min_version('5.6.0')
@pytest.mark.parametrize('record_format', [BASIC_RECORDS, DISCARD_BEFORE_UPDATE_RECORDS, RICH_RECORDS])
@pytest.mark.timeout(180)
def test_record_format_header(sdc_builder, sdc_executor, database, record_format):
    """
    Tests the header of the records includes the parameter `record_format` indicating the record format the records are
    written in.

    The pipeline looks like:
        sql_server_cdc_origin >> wiretap
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
            sqlalchemy.Column('name', sqlalchemy.String(64), primary_key=True),
            sqlalchemy.Column('pokedex_id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('type', sqlalchemy.String(64)),
            sqlalchemy.Column('generation', sqlalchemy.Integer)
        )

        table.create(database.engine)
        capture_instance_name = f'{DEFAULT_SCHEMA_NAME}_{table_name}'
        _enable_cdc(connection, DEFAULT_SCHEMA_NAME, table_name, capture_instance_name)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(
            table_configs=[{'capture_instance': capture_instance_name}],
            record_format=record_format
        )

        wiretap = pipeline_builder.add_wiretap()
        sql_server_cdc >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Define the data for each statement
        initial_data = {'name': 'Azurill', 'pokedex_id': 298, 'type': 'Normal', 'generation': 3}
        updated_data = {'name': 'Azurill', 'pokedex_id': 298, 'type': 'Normal/Fairy', 'generation': 6}

        # Insert some data and update it
        connection.execute(f"""
            insert into {table_name}
            values (
                '{initial_data.get("name")}',
                {initial_data.get("pokedex_id")},
                '{initial_data.get("type")}',
                {initial_data.get("generation")}
            )
        """)

        connection.execute(f"""
            update {table_name}
            set type = '{updated_data.get("type")}', generation = {updated_data.get("generation")}
            where name = '{updated_data.get("name")}' and pokedex_id = {updated_data.get("pokedex_id")}
        """)

        connection.execute(f"delete from {table_name}")

        num_expected_records = 4 if record_format == BASIC_RECORDS else 3

        sdc_executor.start_pipeline(pipeline)
        wait_for_data_in_ct_table(f'{capture_instance_name}_CT', num_expected_records, database)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', num_expected_records)

        assert len(wiretap.output_records) == num_expected_records

        record_format_header = 'record_format'
        record_format_code = _get_record_format_code(record_format)
        output_records = wiretap.output_records

        for index in range(0, num_expected_records):
            header_values = output_records[index].header.values
            assert record_format_header in header_values
            assert header_values[record_format_header] == record_format_code

        sdc_executor.stop_pipeline(pipeline)

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f'drop table if exists {table_name}')

        if pipeline and (sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING'):
            sdc_executor.stop_pipeline(pipeline)


def _get_record_format_code(record_format):
    if record_format == BASIC_RECORDS:
        return BASIC_RECORDS_CODE
    elif record_format == DISCARD_BEFORE_UPDATE_RECORDS:
        return DISCARD_BEFORE_UPDATE_RECORDS_CODE
    else:
        return RICH_RECORDS_CODE


def _sort_records(entry):
    return entry.field['id'].value


def _sort_combined_records(entry):
    return entry.field['Data']['id'].value


def _enable_cdc(connection, schema_name, table_name, capture_instance=None):
    if capture_instance is None:
        capture_instance = f"{schema_name}_{table_name}"

    logger.info('Enabling CDC on %s.%s into table %s...', schema_name, table_name, capture_instance)
    connection.execute(f'EXEC sys.sp_cdc_enable_table '
                       f'@source_schema=N\'{schema_name}\', '
                       f'@source_name=N\'{table_name}\','
                       f'@role_name = NULL, '
                       f'@capture_instance={capture_instance}')

    _wait_until_is_tracked_by_cdc(connection, table_name, 1)


def _disable_cdc(connection, schema_name, table_name, capture_instance=None):
    if capture_instance is None:
        capture_instance = f"{schema_name}_{table_name}"

    logger.info('Disabling CDC on %s.%s from table %s...', schema_name, table_name, capture_instance)
    connection.execute(
        f'EXEC sys.sp_cdc_disable_table '
        f'@source_schema=N\'{schema_name}\', '
        f'@source_name=N\'{table_name}\','
        f'@capture_instance={capture_instance}')

    _wait_until_is_tracked_by_cdc(connection, table_name, 0)


def _wait_until_is_tracked_by_cdc(connection, table_name, is_tracked_by_cdc):
    while True:
        cursor = connection.execute(
            f"select name from sys.tables where is_tracked_by_cdc = {is_tracked_by_cdc} and name = '{table_name}'"
        )
        if len(cursor.fetchall()) > 0:
            break
        else:
            logger.info(f'Waiting until CDC is enabled/disabled for the table {table_name}')
            sleep(1)


def _get_data_field_from_record(record, field_name, record_format=BASIC_RECORDS):
    return record.field['Data'][field_name] if record_format == RICH_RECORDS else record.field[field_name]


def _check_record(record, sdc_operation_type, id, name, dt, record_format, combine_update_records_field=None):
    assert record.header.values['sdc.operation.type'] == sdc_operation_type
    record_field_data = record.field[combine_update_records_field] if record_format == RICH_RECORDS else record.field
    assert record_field_data['id'].value == id
    assert record_field_data['name'].value == name
    assert record_field_data['dt'].value == dt
