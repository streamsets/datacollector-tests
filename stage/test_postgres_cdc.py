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
from streamsets.sdk.utils import Version
from streamsets.sdk.utils import wait_for_condition
from streamsets.testframework.environments import databases
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

table_column_id = 'id'
table_column_name = 'name'

data_for_operations = namedtuple('OperationsData', ['kind', 'table', 'columnnames', 'columnvalues', 'oldkeys'])
old_keys = namedtuple('Oldkeys', ['keynames', 'keyvalues'])

rows_to_insert = [{table_column_id: 0, table_column_name: 'Mbappe'},
                  {table_column_id: 1, table_column_name: 'Kane'},
                  {table_column_id: 2, table_column_name: 'Griezmann'}]

rows_to_update = [{table_column_id: 0, table_column_name: 'Kylian Mbappe'},
                  {table_column_id: 1, table_column_name: 'Harry Kane'},
                  {table_column_id: 2, table_column_name: 'Antoine Griezmann'}]

rows_to_delete = [{table_column_id: 0},
                  {table_column_id: 1},
                  {table_column_id: 2}]

insert_kind = 'insert'
update_kind = 'update'
delete_kind = 'delete'

check_replication_slot_query = 'select slot_name from pg_replication_slots;'

inserts_per_thread = 20
threads_count = 10
threaded_records = inserts_per_thread * threads_count

wal_sender_columns_blacklist = set(["usename", "client_addr", "client_hostname"])
wal_sender_columns_whitelist = set(["application_name", "pid"])


def _create_table_in_database(table_name, database):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column(table_column_id, sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(table_column_name, sqlalchemy.String(20))
    )
    logger.info('Creating table %s in %s database ...', table_name, database.type)
    table.create(database.engine)
    return table


def _insert(connection, table, insert_rows=rows_to_insert, create_txn=False):
    if create_txn:
        txn = connection.begin()

    connection.execute(table.insert(), insert_rows)
    logger.info('Inserting rows %s in %s table ...', insert_rows, table)

    if create_txn:
        txn.commit()

    # Prepare expected data to compare for verification against wiretap data.
    operations_data = []
    for row in insert_rows:
        operations_data.append(data_for_operations(insert_kind,
                                                   table.name,
                                                   [table_column_id, table_column_name],
                                                   list(row.values()),
                                                   None))  # No oldkeys expected.
    if create_txn:
        txn.close()

    return operations_data


def _update(connection, table, update_rows=rows_to_update):
    operations_data = []
    txn = connection.begin()

    try:
        for row in update_rows:
            connection.execute(table.update().where(table.c.id == row[table_column_id]).values(name=row[table_column_name]))
            logger.info('Updating row %s from %s table ...', row, table)
            # Prepare expected data to compare for verification against wiretap data.
            operations_data.append(data_for_operations(update_kind,
                                                       table.name,
                                                       [table_column_id, table_column_name],
                                                       list(row.values()),
                                                       old_keys([table_column_id], [row[table_column_id]])))
        txn.commit()


    except:
        txn.rollback()
        pytest.fail('Error updating rows ...')

    finally:
        txn.close()

    return operations_data


def _delete(connection, table, delete_rows=rows_to_delete):
    txn = connection.begin()
    operations_data = []
    try:
        for row in delete_rows:
            connection.execute(table.delete().where(table.c.id == row[table_column_id]))
            logger.info('Deleting row %s from %s table ...', row, table)
            # Prepare expected data to compare for verification against wiretap data.
            operations_data.append(data_for_operations(delete_kind,
                                                       table.name,
                                                       None,  # No columnnames expected.
                                                       None,  # No columnvalues expected.
                                                       old_keys([table_column_id], [row[table_column_id]])))
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


def transaction_data_to_operation_data(transaction_data, wal2json_format, record_contents):
    wal2json_version = 2 if wal2json_format == 'OPERATION' and record_contents == 'OPERATION' else 1
    operations_data = []
    for operation in transaction_data:
        row_data = {}
        if operation.columnnames is not None and operation.columnvalues is not None:
            for column_name, column_value in zip(operation.columnnames, operation.columnvalues):
                row_data[column_name] = column_value
        primary_key_data = {}
        if operation.oldkeys is not None:
            for column_name, column_value in zip(operation.oldkeys.keynames, operation.oldkeys.keyvalues):
                primary_key_data[column_name] = column_value
        operation_code = operation.kind.upper() if wal2json_version == 1 else operation.kind.upper()[0]
        operations_data.append({'operation': operation_code,
                                'schema': 'public',
                                'table': operation.table,
                                'row_data': row_data,
                                'primary_key_data': primary_key_data})
    return operations_data


@database('postgresql')
@sdc_min_version('3.16.0')
@pytest.mark.parametrize('poll_interval', [1,
                                           5])
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_stop_start(sdc_builder,
                    sdc_executor,
                    database,
                    poll_interval,
                    wal2json_format,
                    record_contents):

    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and wal2json_format == 'OPERATION':
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    sample_data = [dict(id=i, name=f'Takemiya{i}') for i in range(40)]

    table_name = get_random_string(string.ascii_lowercase, 20)
    table = sqlalchemy.Table(table_name,
                             sqlalchemy.MetaData(),
                             sqlalchemy.Column('id',
                                               sqlalchemy.Integer,
                                               primary_key=True),
                             sqlalchemy.Column('name',
                                               sqlalchemy.String(20)))
    replication_slot = get_random_string(string.ascii_lowercase, 10)

    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()
        postgresql_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
        postgresql_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                             max_batch_size_in_records=1,
                                             poll_interval=poll_interval,
                                             replication_slot=replication_slot)
        if Version(sdc_builder.version) >= Version('4.2.0'):
            postgresql_cdc_client.set_attributes(ssl_mode='DISABLED')
        if Version(sdc_builder.version) >= Version('5.1.0'):
            postgresql_cdc_client.set_attributes(record_contents=record_contents,
                                                 wal2json_format=wal2json_format)
        wiretap = pipeline_builder.add_wiretap()
        pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
        if record_contents == 'TRANSACTION':
            precondition = "${record:value('/change[0]/columnvalues[0]') == 9 " \
                          "or record:value('/change[0]/columnvalues[0]') == 19 " \
                          "or record:value('/change[0]/columnvalues[0]') == 29 " \
                          "or record:value('/change[0]/columnvalues[0]') == 39}"
        else:
            precondition = "${record:value('/id') == 9 " \
                          "or record:value('/id') == 19 " \
                          "or record:value('/id') == 29 " \
                          "or record:value('/id') == 39}"
        pipeline_finisher.set_attributes(preconditions=[precondition])
        postgresql_cdc_client >> [wiretap.destination, pipeline_finisher]
        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Starting pipeline for the first time ...')
        sdc_executor.start_pipeline(pipeline)
        table.create(database.engine)
        with database.engine.connect().execution_options(autocommit=True) as connection:
            for row in sample_data[:30]:
                connection.execute(table.insert(), row)
        sdc_executor.wait_for_pipeline_status(pipeline, 'FINISHED', timeout_sec=300)
        if record_contents == 'TRANSACTION':
            assert [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                    for record in wiretap.output_records] == sample_data[:10]
        else:
            assert [{'id': record.field['id'], 'name': record.field['name']}
                    for record in wiretap.output_records] == sample_data[:10]
        wiretap.reset()

        logger.info('Starting pipeline for the second time ...')
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=300)
        if record_contents == 'TRANSACTION':
            assert [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                    for record in wiretap.output_records] == sample_data[10:20]
        else:
            assert [{'id': record.field['id'], 'name': record.field['name']}
                    for record in wiretap.output_records] == sample_data[10:20]
        wiretap.reset()

        logger.info('Starting pipeline for the third time ...')
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=300)
        if record_contents == 'TRANSACTION':
            assert [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                    for record in wiretap.output_records] == sample_data[20:30]
        else:
            assert [{'id': record.field['id'], 'name': record.field['name']}
                    for record in wiretap.output_records] == sample_data[20:30]
        wiretap.reset()

        logger.info('Starting pipeline for the fourth time ...')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)
        metrics = sdc_executor.get_pipeline_history(pipeline).latest.metrics
        assert metrics.counter("pipeline.batchOutputRecords.counter").count == 0
        with database.engine.connect().execution_options(autocommit=True) as connection:
            for row in sample_data[30:40]:
                connection.execute(table.insert(), row)
        wiretap.reset()

        logger.info('Starting pipeline for the fifth time ...')
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=300)
        if record_contents == 'TRANSACTION':
            assert [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                    for record in wiretap.output_records] == sample_data[30:40]
        else:
            assert [{'id': record.field['id'], 'name': record.field['name']}
                    for record in wiretap.output_records] == sample_data[30:40]
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot)
        if table is not None:
            table.drop(database.engine)
        database.engine.connect().close()


@database('postgresql')
@sdc_min_version('3.16.0')
@pytest.mark.parametrize('start_from', ['DATE', 'LSN'])
@pytest.mark.parametrize('create_slot', [True, False])
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_start_not_from_latest(sdc_builder,
                               sdc_executor,
                               database,
                               start_from,
                               create_slot,
                               wal2json_format,
                               record_contents):

    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and wal2json_format == 'OPERATION':
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    if start_from is 'LSN' and database.database_server_version.major < 10:
        pytest.skip('LSN test cannot be executed in versions PostgresSQL versions < 10.')

    sample_data_1 = [dict(id=f'1{i}', name=f'Takemiya_{i}') for i in range(20)]
    sample_data_2 = [dict(id=f'2{i}', name=f'Kobayashi_{i}') for i in range(20)]
    sample_data_3 = [dict(id=f'3{i}', name=f'Ishida_{i}') for i in range(20)]
    sample_data_4 = [dict(id=f'4{i}', name=f'Otake_{i}') for i in range(20)]

    table_name = get_random_string(string.ascii_lowercase, 20)
    table = sqlalchemy.Table(table_name,
                             sqlalchemy.MetaData(),
                             sqlalchemy.Column('id',
                                               sqlalchemy.String(20),
                                               primary_key=True),
                             sqlalchemy.Column('name',
                                               sqlalchemy.String(20)))
    replication_slot = get_random_string(string.ascii_lowercase, 10)

    try:
        table.create(database.engine)
        if create_slot:
            with database.engine.connect().execution_options(autocommit=True) as connection:
                connection.execute(
                    f'select * from pg_create_logical_replication_slot(\'{replication_slot}\', \'wal2json\')')
        with database.engine.connect().execution_options(autocommit=True) as connection:
            for row in sample_data_1:
                connection.execute(table.insert(), row)
        if start_from is 'DATE':
            time.sleep(5)
            with database.engine.connect().execution_options(autocommit=True) as connection:
                date = connection.execute('select current_timestamp').first()[0]
                timezone = str(connection.execute('show timezone').first()[0])
        else:
            with database.engine.connect().execution_options(autocommit=True) as connection:
                start_lsn = str(connection.execute('select pg_current_wal_lsn()').first()[0])
        with database.engine.connect().execution_options(autocommit=True) as connection:
            for row in sample_data_2:
                connection.execute(table.insert(), row)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        postgresql_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
        postgresql_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                             max_batch_size_in_records=1,
                                             replication_slot=replication_slot,
                                             initial_change=start_from,
                                             poll_interval=1)
        if start_from is 'DATE':
            postgresql_cdc_client.set_attributes(start_date=date.strftime('%m-%d-%Y %H:%M:%S'),
                                                 database_time_zone=timezone)
        else:
            postgresql_cdc_client.set_attributes(start_lsn=start_lsn)
        if Version(sdc_builder.version) >= Version('4.2.0'):
            postgresql_cdc_client.set_attributes(ssl_mode='DISABLED')
        if Version(sdc_builder.version) >= Version('5.1.0'):
            postgresql_cdc_client.set_attributes(record_contents=record_contents,
                                                 wal2json_format=wal2json_format)
        wiretap = pipeline_builder.add_wiretap()
        pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
        if record_contents == 'TRANSACTION':
            precondition = "${record:value('/change[0]/columnvalues[0]') == 219 " \
                          "or record:value('/change[0]/columnvalues[0]') == 319 " \
                          "or record:value('/change[0]/columnvalues[0]') == 419}"
        else:
            precondition = "${record:value('/id') == 219 " \
                          "or record:value('/id') == 319 " \
                          "or record:value('/id') == 419}"
        pipeline_finisher.set_attributes(preconditions=[precondition])
        postgresql_cdc_client >> [wiretap.destination, pipeline_finisher]
        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        if create_slot:
            expected_data = sample_data_2
        else:
            expected_data = sample_data_4
            sdc_executor.wait_for_pipeline_status(pipeline, 'RUNNING', timeout_sec=300)
            with database.engine.connect().execution_options(autocommit=True) as connection:
                for row in sample_data_4:
                    connection.execute(table.insert(), row)
        sdc_executor.wait_for_pipeline_status(pipeline, 'FINISHED', timeout_sec=300)
        if record_contents == 'TRANSACTION':
            assert [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                    for record in wiretap.output_records] == expected_data
        else:
            assert [{'id': record.field['id'], 'name': record.field['name']}
                    for record in wiretap.output_records] == expected_data
        wiretap.reset()

        with database.engine.connect().execution_options(autocommit=True) as connection:
            for row in sample_data_3:
                connection.execute(table.insert(), row)
            sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_status(pipeline, 'FINISHED', timeout_sec=300)
        if record_contents == 'TRANSACTION':
            assert [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                    for record in wiretap.output_records] == sample_data_3
        else:
            assert [{'id': record.field['id'], 'name': record.field['name']}
                    for record in wiretap.output_records] == sample_data_3
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot)
        if table is not None:
            table.drop(database.engine)
        database.engine.connect().close()


@database('postgresql')
@sdc_min_version('3.4.0')
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_postgres_cdc_client_basic(sdc_builder,
                                   sdc_executor,
                                   database,
                                   wal2json_format,
                                   record_contents):

    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and wal2json_format == 'OPERATION':
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    postgres_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                       max_batch_size_in_records=1,
                                       remove_replication_slot_on_close=True,
                                       poll_interval=1,
                                       replication_slot=replication_slot_name)
    if Version(sdc_builder.version) >= Version('4.2.0'):
        postgres_cdc_client.set_attributes(ssl_mode='DISABLED')
    if Version(sdc_builder.version) >= Version('5.1.0'):
        postgres_cdc_client.set_attributes(record_contents=record_contents,
                                           wal2json_format=wal2json_format)
    wiretap = pipeline_builder.add_wiretap()
    postgres_cdc_client >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = None
    try:
        sdc_executor.start_pipeline(pipeline)
        table = _create_table_in_database(table_name, database)
        connection = database.engine.connect()
        expected_operations_data = _insert(connection=connection, table=table)
        expected_operations_data += _update(connection=connection, table=table)
        expected_operations_data += _delete(connection=connection, table=table)

        if record_contents == 'TRANSACTION':
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3, timeout_sec=300)
        else:
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 9, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)
        if record_contents == 'TRANSACTION':
            assert len(wiretap.output_records) == 3
        else:
            assert len(wiretap.output_records) == 9
        for record in wiretap.output_records:
            logger.info(f'Record :: {record}')
        if record_contents == 'TRANSACTION':
            operation_index = 0
            for record in wiretap.output_records:
                if record.get_field_data('/change'):
                    for i in range(3):
                        expected = expected_operations_data[operation_index]
                        assert expected.kind == record.get_field_data(f'/change[{i}]/kind')
                        assert expected.table == record.get_field_data(f'/change[{i}]/table')
                        if expected.kind != delete_kind:
                            assert expected.columnnames == record.get_field_data(f'/change[{i}]/columnnames')
                            assert expected.columnvalues == record.get_field_data(f'/change[{i}]/columnvalues')
                        if expected.kind != insert_kind:
                            assert expected.oldkeys.keynames == record.get_field_data(f'/change[{i}]/oldkeys/keynames')
                            assert expected.oldkeys.keyvalues == record.get_field_data(f'/change[{i}]/oldkeys/keyvalues')
                        operation_index += 1
            assert operation_index == len(expected_operations_data)
        else:
            expected_operations_data = transaction_data_to_operation_data(expected_operations_data,
                                                                          wal2json_format,
                                                                          record_contents)
            for record, expected in zip(wiretap.output_records, expected_operations_data):
                assert expected['operation'] == record.header.values['postgres.cdc.operation']
                assert expected['schema'] == record.header.values['postgres.cdc.schema']
                assert expected['table'] == record.header.values['postgres.cdc.table']
                if expected['operation'] != delete_kind.upper():
                    for expected_column_name, expected_column_value in expected['row_data'].items():
                        assert record.field[expected_column_name] == expected_column_value
                if expected['operation'] != insert_kind.upper():
                    for expected_column_name, expected_column_value in expected['primary_key_data'].items():
                        assert record.field[expected_column_name] == expected_column_value

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table is not None:
            table.drop(database.engine)
        connection.close()


@database('postgresql')
@sdc_min_version('3.4.0')
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_postgres_cdc_max_poll_attempts(sdc_builder,
                                        sdc_executor,
                                        database,
                                        wal2json_format,
                                        record_contents):

    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and wal2json_format == 'OPERATION':
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    postgres_cdc_client.set_attributes(batch_wait_time_in_ms=30000,
                                       max_batch_size_in_records=33,
                                       remove_replication_slot_on_close=True,
                                       poll_interval=1,
                                       replication_slot=replication_slot_name)
    if Version(sdc_builder.version) >= Version('4.2.0'):
        postgres_cdc_client.set_attributes(ssl_mode='DISABLED')
    if Version(sdc_builder.version) >= Version('5.1.0'):
        postgres_cdc_client.set_attributes(record_contents=record_contents,
                                           wal2json_format=wal2json_format)
    wiretap = pipeline_builder.add_wiretap()
    postgres_cdc_client >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = None
    try:
        sdc_executor.start_pipeline(pipeline)
        table = _create_table_in_database(table_name, database)
        connection = database.engine.connect()
        expected_operations_data = _insert(connection=connection, table=table)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'data_batch_count', 1, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        if record_contents == 'TRANSACTION':
            assert len(wiretap.output_records) == 1
        else:
            assert len(wiretap.output_records) == 3
        if record_contents == 'TRANSACTION':
            operation_index = 0
            for record in wiretap.output_records:
                if record.get_field_data('/change'):
                    for i in range(3):
                        expected = expected_operations_data[operation_index]
                        assert expected.kind == record.get_field_data(f'/change[{i}]/kind')
                        assert expected.table == record.get_field_data(f'/change[{i}]/table')
                        if expected.kind != delete_kind:
                            assert expected.columnnames == record.get_field_data(f'/change[{i}]/columnnames')
                            assert expected.columnvalues == record.get_field_data(f'/change[{i}]/columnvalues')
                        if expected.kind != insert_kind:
                            assert expected.oldkeys.keynames == record.get_field_data(f'/change[{i}]/oldkeys/keynames')
                            assert expected.oldkeys.keyvalues == record.get_field_data(f'/change[{i}]/oldkeys/keyvalues')
                        operation_index += 1
            assert operation_index == len(expected_operations_data)
        else:
            expected_operations_data = transaction_data_to_operation_data(expected_operations_data,
                                                                          wal2json_format,
                                                                          record_contents)
            for record, expected in zip(wiretap.output_records, expected_operations_data):
                assert expected['operation'] == record.header.values['postgres.cdc.operation']
                assert expected['schema'] == record.header.values['postgres.cdc.schema']
                assert expected['table'] == record.header.values['postgres.cdc.table']
                if expected['operation'] != delete_kind.upper():
                    for expected_column_name, expected_column_value in expected['row_data'].items():
                        assert record.field[expected_column_name] == expected_column_value
                if expected['operation'] != insert_kind.upper():
                    for expected_column_name, expected_column_value in expected['primary_key_data'].items():
                        assert record.field[expected_column_name] == expected_column_value

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table is not None:
            table.drop(database.engine)
        connection.close()


@database('postgresql')
@sdc_min_version('3.8.1')
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_postgres_cdc_client_filtering_table(sdc_builder,
                                             sdc_executor,
                                             database,
                                             wal2json_format,
                                             record_contents):

    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and wal2json_format == 'OPERATION':
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    table_name_allow = get_random_string(string.ascii_lowercase, 20)
    table_name_deny = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    postgres_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                       max_batch_size_in_records=1,
                                       remove_replication_slot_on_close=True,
                                       poll_interval=1,
                                       replication_slot=replication_slot_name,
                                       tables=[{'schema': 'public',
                                                'excludePattern': table_name_deny,
                                                'table': table_name_allow}])
    if Version(sdc_builder.version) >= Version('4.2.0'):
        postgres_cdc_client.set_attributes(ssl_mode='DISABLED')
    if Version(sdc_builder.version) >= Version('5.1.0'):
        postgres_cdc_client.set_attributes(record_contents=record_contents,
                                           wal2json_format=wal2json_format)

    wiretap = pipeline_builder.add_wiretap()
    postgres_cdc_client >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table_allow = _create_table_in_database(table_name_allow, database)
    table_deny = _create_table_in_database(table_name_deny, database)
    try:
        sdc_executor.start_pipeline(pipeline)
        connection = database.engine.connect()
        _insert(connection=connection, table=table_deny)
        _update(connection=connection, table=table_deny)
        _delete(connection=connection, table=table_deny)
        expected_operations_data = _insert(connection=connection, table=table_allow)
        expected_operations_data += _update(connection=connection, table=table_allow)
        expected_operations_data += _delete(connection=connection, table=table_allow)

        if record_contents == 'TRANSACTION':
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3, timeout_sec=300)
        else:
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 9, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)
        if record_contents == 'TRANSACTION':
            assert len(wiretap.output_records) == 3
        else:
            assert len(wiretap.output_records) == 9
        for record in wiretap.output_records:
            logger.info(f'Record :: {record}')
        if record_contents == 'TRANSACTION':
            operation_index = 0
            for record in wiretap.output_records:
                if record.get_field_data('/change'):
                    for i in range(3):
                        expected = expected_operations_data[operation_index]
                        assert expected.kind == record.get_field_data(f'/change[{i}]/kind')
                        assert expected.table == record.get_field_data(f'/change[{i}]/table')
                        if expected.kind != delete_kind:
                            assert expected.columnnames == record.get_field_data(f'/change[{i}]/columnnames')
                            assert expected.columnvalues == record.get_field_data(f'/change[{i}]/columnvalues')
                        if expected.kind != insert_kind:
                            assert expected.oldkeys.keynames == record.get_field_data(f'/change[{i}]/oldkeys/keynames')
                            assert expected.oldkeys.keyvalues == record.get_field_data(f'/change[{i}]/oldkeys/keyvalues')
                        operation_index += 1
            assert operation_index == len(expected_operations_data)
        else:
            expected_operations_data = transaction_data_to_operation_data(expected_operations_data,
                                                                          wal2json_format,
                                                                          record_contents)
            for record, expected in zip(wiretap.output_records, expected_operations_data):
                assert expected['operation'] == record.header.values['postgres.cdc.operation']
                assert expected['schema'] == record.header.values['postgres.cdc.schema']
                assert expected['table'] == record.header.values['postgres.cdc.table']
                if expected['operation'] != delete_kind.upper():
                    for expected_column_name, expected_column_value in expected['row_data'].items():
                        assert record.field[expected_column_name] == expected_column_value
                if expected['operation'] != insert_kind.upper():
                    for expected_column_name, expected_column_value in expected['primary_key_data'].items():
                        assert record.field[expected_column_name] == expected_column_value

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table_allow is not None:
            table_allow.drop(database.engine)
        if table_deny is not None:
            table_deny.drop(database.engine)
        connection.close()


@database('postgresql')
@sdc_min_version('3.4.0')
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_postgres_cdc_client_remove_replication_slot(sdc_builder,
                                                     sdc_executor,
                                                     database,
                                                     wal2json_format,
                                                     record_contents):

    if database.database_server_version < databases.EARLIEST_POSTGRESQL_VERSION_WITH_ACTIVE_PID:
        pytest.skip('Test only runs against PostgresSQL version >= '
                    f"{'.'.join(str(item) for item in databases.EARLIEST_POSTGRESQL_VERSION_WITH_ACTIVE_PID)}")

    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and wal2json_format == 'OPERATION':
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    table_name = get_random_string(string.ascii_lowercase, 20)
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
    postgres_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                       max_batch_size_in_records=1,
                                       remove_replication_slot_on_close=True,
                                       poll_interval=1,
                                       replication_slot=replication_slot_name)
    if Version(sdc_builder.version) >= Version('4.2.0'):
        postgres_cdc_client.set_attributes(ssl_mode='DISABLED')
    if Version(sdc_builder.version) >= Version('5.1.0'):
        postgres_cdc_client.set_attributes(record_contents=record_contents,
                                           wal2json_format=wal2json_format)
    wiretap = pipeline_builder.add_wiretap()
    postgres_cdc_client >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = None
    try:
        sdc_executor.start_pipeline(pipeline)
        table = _create_table_in_database(table_name, database)
        connection = database.engine.connect()
        expected_operations_data = _insert(connection=connection, table=table)
        expected_operations_data += _update(connection=connection, table=table)
        expected_operations_data += _delete(connection=connection, table=table)
        sdc_executor.stop_pipeline(pipeline=pipeline).wait_for_stopped(timeout_sec=60)

        listed_slots = connection.execute(check_replication_slot_query).fetchall()
        logger.info('Replication slot:  ' + replication_slot_name)
        logger.info('List of current slots: ' + str(listed_slots))
        assert (replication_slot_name,) not in listed_slots

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        if table is not None:
            table.drop(database.engine)
        connection.close()


@database('postgresql')
@sdc_min_version('3.16.0')
@pytest.mark.parametrize('batch_size', [1,
                                        10,
                                        100,
                                        1000])
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_postgres_cdc_client_multiple_concurrent_operations(sdc_builder,
                                                            sdc_executor,
                                                            database,
                                                            batch_size,
                                                            wal2json_format,
                                                            record_contents):

    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and wal2json_format == 'OPERATION':
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    if record_contents == 'TRANSACTION':
        max_batch_size_in_records = batch_size
        batch_wait_time_in_ms = 3000
        timeout_sec = 120
    else:
        max_batch_size_in_records = 10 * batch_size
        batch_wait_time_in_ms = 30000
        timeout_sec = 300
    postgres_cdc_client.set_attributes(batch_wait_time_in_ms=batch_wait_time_in_ms,
                                       max_batch_size_in_records=max_batch_size_in_records,
                                       remove_replication_slot_on_close=False,
                                       poll_interval=1,
                                       replication_slot=replication_slot_name)
    if Version(sdc_builder.version) >= Version('4.2.0'):
        postgres_cdc_client.set_attributes(ssl_mode='DISABLED')
    if Version(sdc_builder.version) >= Version('5.1.0'):
        postgres_cdc_client.set_attributes(record_contents=record_contents,
                                           wal2json_format=wal2json_format)
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    if record_contents == 'TRANSACTION':
        precondition = "${record:value('/change[0]/columnvalues[0]') == -1}"
    else:
        precondition = "${record:value('/id') == -1}"
    pipeline_finisher.set_attributes(preconditions=[precondition])
    postgres_cdc_client >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = None
    try:
        pipeline_cmd = sdc_executor.start_pipeline(pipeline)
        table = _create_table_in_database(table_name, database)
        connections = [database.engine.connect() for _ in range(threads_count)]
        expected = []
        def inserter_thread(connection, table, id, amount):
            for i in range(amount):
                insert_rows = [{table_column_id: id * amount + i,
                                table_column_name: get_random_string(string.ascii_lowercase, 10)}]
                expected.append(_insert(connection=connection,
                                        table=table,
                                        insert_rows=insert_rows,
                                        create_txn=True))
                insert_rows = [{table_column_id: id * amount + i,
                                table_column_name: get_random_string(string.ascii_lowercase, 10)}]
                expected.append(_update(connection=connection,
                                        table=table,
                                        update_rows=insert_rows))
                expected.append(_delete(connection=connection,
                                        table=table,
                                        delete_rows=insert_rows))
        thread_pool = [threading.Thread(target=inserter_thread,
                                        args=(connections[i],
                                              table,
                                              i,
                                              inserts_per_thread)) for i in range(threads_count)]
        for thread in thread_pool:
            thread.start()
        for thread in thread_pool:
            thread.join()
        final_row = [{table_column_id: -1, table_column_name: 'Last Record'}]
        expected.append(_insert(connection=connections[0],
                                table=table,
                                insert_rows=final_row,
                                create_txn=True))
        pipeline_cmd.wait_for_finished(timeout_sec=timeout_sec)

        output_values = []
        if record_contents == 'TRANSACTION':
            for record in wiretap.output_records:
                if record.get_field_data('/change[0]/kind') == 'delete':
                    output_values.append({'type': 'delete', 'value': record.get_field_data('/change[0]/oldkeys/keyvalues')})
                if record.get_field_data('/change[0]/kind') == 'insert':
                    output_values.append({'type': 'insert', 'value': record.get_field_data('/change[0]/columnvalues')})
                if record.get_field_data('/change[0]/kind') == 'update':
                    output_values.append({'type': 'update', 'value': record.get_field_data('/change[0]/columnvalues')})
            output_sorted_values = sorted(output_values, key=lambda key: f'{key["value"][0]}|{key["type"]}')
        else:
            for record in wiretap.output_records:
                if record.header.values['postgres.cdc.operation'] == 'DELETE' or   record.header.values['postgres.cdc.operation'] == 'D':
                    output_values.append({'type': 'delete', 'value': record.field['id']})
                if record.header.values['postgres.cdc.operation'] == 'INSERT' or record.header.values['postgres.cdc.operation'] == 'I':
                    output_values.append({'type': 'insert', 'value': record.field['id']})
                elif record.header.values['postgres.cdc.operation'] == 'UPDATE' or record.header.values['postgres.cdc.operation'] == 'U':
                    output_values.append({'type': 'update', 'value': record.field['id']})
            output_sorted_values = sorted(output_values, key=lambda key: f'{key["value"]}|{key["type"]}')

        expected_values = []
        if record_contents == 'TRANSACTION':
            for record in expected:
                if record[0].kind == 'delete':
                    expected_values.append({'type': 'delete', 'value': record[0].oldkeys.keyvalues})
                if record[0].kind == 'insert':
                    expected_values.append({'type': 'insert', 'value': record[0].columnvalues})
                if record[0].kind == 'update':
                    expected_values.append({'type': 'update', 'value': record[0].columnvalues})
            expected_sorted_values = sorted(expected_values, key=lambda key: f'{key["value"][0]}|{key["type"]}')
        else:
            for record in expected:
                if record[0].kind == 'delete':
                    for operation in record:
                        expected_values.append({'type': 'delete', 'value': operation.oldkeys.keyvalues[0]})
                if record[0].kind == 'insert':
                    for operation in record:
                        expected_values.append({'type': 'insert', 'value': operation.columnvalues[0]})
                if record[0].kind == 'update':
                    for operation in record:
                        expected_values.append({'type': 'update', 'value': operation.columnvalues[0]})
            expected_sorted_values = sorted(expected_values, key=lambda key: f'{key["value"]}|{key["type"]}')

        assert len(expected_sorted_values) == len(output_sorted_values)
        assert expected_sorted_values == output_sorted_values

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table is not None:
            table.drop(database.engine)
        for conn in connections:
            conn.close()


@database('postgresql')
@sdc_min_version('3.8.1')
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_postgres_cdc_client_filtering_multiple_tables(sdc_builder,
                                                       sdc_executor,
                                                       database,
                                                       wal2json_format,
                                                       record_contents):

    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and wal2json_format == 'OPERATION':
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')


    table_name = [get_random_string(string.ascii_lowercase, 20) for _ in range(4)]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    postgres_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                       max_batch_size_in_records=1,
                                       remove_replication_slot_on_close=True,
                                       replication_slot=replication_slot_name,
                                       poll_interval=1,
                                       tables=[{'schema': 'public',
                                                'excludePattern': '',
                                                'table': table_name[i]} for i in range(3)])
    if Version(sdc_builder.version) >= Version('4.2.0'):
        postgres_cdc_client.set_attributes(ssl_mode='DISABLED')
    if Version(sdc_builder.version) >= Version('5.1.0'):
        postgres_cdc_client.set_attributes(record_contents=record_contents,
                                           wal2json_format=wal2json_format)
    wiretap = pipeline_builder.add_wiretap()
    postgres_cdc_client >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = []
    try:
        table = [_create_table_in_database(name, database) for name in table_name]
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
            _insert(connection=connection, table=table[3], create_txn=True)
            _update(connection=connection, table=table[3])
            _delete(connection=connection, table=table[3])
        if record_contents == 'TRANSACTION':
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 9, timeout_sec=300)
        else:
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 27, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)
        if record_contents == 'TRANSACTION':
            assert len(wiretap.output_records) == 9
        else:
            assert len(wiretap.output_records) == 27

        if record_contents == 'TRANSACTION':
            operation_index = 0
            for record in wiretap.output_records:
                if record.get_field_data('/change'):
                    for i in range(3):
                        expected = expected_operations_data[operation_index]
                        assert expected.kind == record.get_field_data(f'/change[{i}]/kind')
                        assert expected.table == record.get_field_data(f'/change[{i}]/table')
                        if expected.kind != delete_kind:
                            assert expected.columnnames == record.get_field_data(f'/change[{i}]/columnnames')
                            assert expected.columnvalues == record.get_field_data(f'/change[{i}]/columnvalues')
                        if expected.kind != insert_kind:
                            assert expected.oldkeys.keynames == record.get_field_data(f'/change[{i}]/oldkeys/keynames')
                            assert expected.oldkeys.keyvalues == record.get_field_data(f'/change[{i}]/oldkeys/keyvalues')
                        operation_index += 1
            assert operation_index == len(expected_operations_data)
        else:
            expected_operations_data = transaction_data_to_operation_data(expected_operations_data,
                                                                          wal2json_format,
                                                                          record_contents)
            for record, expected in zip(wiretap.output_records, expected_operations_data):
                assert expected['operation'] == record.header.values['postgres.cdc.operation']
                assert expected['schema'] == record.header.values['postgres.cdc.schema']
                assert expected['table'] == record.header.values['postgres.cdc.table']
                if expected['operation'] != delete_kind.upper():
                    for expected_column_name, expected_column_value in expected['row_data'].items():
                        assert record.field[expected_column_name] == expected_column_value
                if expected['operation'] != insert_kind.upper():
                    for expected_column_name, expected_column_value in expected['primary_key_data'].items():
                        assert record.field[expected_column_name] == expected_column_value

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        for t in table:
            t.drop(database.engine)
        database.engine.connect().close()


@database('postgresql')
@sdc_min_version('3.21.0')
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_postgres_cdc_wal_sender_status_metrics(sdc_builder,
                                                sdc_executor,
                                                database,
                                                wal2json_format,
                                                record_contents):

    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and wal2json_format == 'OPERATION':
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    postgres_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                       max_batch_size_in_records=1,
                                       remove_replication_slot_on_close=True,
                                       replication_slot=replication_slot_name,
                                       poll_interval=1,
                                       tables=[{'schema': 'public',
                                                'excludePattern': '',
                                                'table': table_name}])
    if Version(sdc_builder.version) >= Version('4.2.0'):
        postgres_cdc_client.set_attributes(ssl_mode='DISABLED')
    if Version(sdc_builder.version) >= Version('5.1.0'):
        postgres_cdc_client.set_attributes(record_contents=record_contents,
                                           wal2json_format=wal2json_format)
    trash = pipeline_builder.add_stage('Trash')
    postgres_cdc_client >> trash
    pipeline = pipeline_builder.build().configure_for_environment(database)
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
        if record_contents == 'TRANSACTION':
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3, timeout_sec=300)
        else:
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 9, timeout_sec=300)
        wal_sender_status_from_db = _get_wal_sender_status(connection)
        assert wal_sender_status_from_db is not None
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        wal_sender_status_from_metrics = \
            history.latest.metrics.gauge('custom.PostgreSQLCDCClient_01.Wal Sender Status.0.gauge').value
        assert all([k not in wal_sender_status_from_metrics for k in wal_sender_columns_blacklist])
        assert ({k: str(v) for k, v in wal_sender_status_from_metrics.items() if k in wal_sender_columns_whitelist} ==
                {k: str(v) for k, v in wal_sender_status_from_db.items() if k in wal_sender_columns_whitelist})
    finally:
        if sdc_executor.get_pipeline_status(pipeline) == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        table.drop(database.engine)
        connection.close()


@database('postgresql')
@sdc_min_version('3.21.0')
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_postgres_cdc_queue_buffering_metrics(sdc_builder,
                                              sdc_executor,
                                              database,
                                              wal2json_format,
                                              record_contents):

    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and wal2json_format == 'OPERATION':
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    table_names = [get_random_string(string.ascii_lowercase, 20) for _ in range(9)]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    queue_size = 6
    postgres_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                       max_batch_size_in_records=1,
                                       remove_replication_slot_on_close=True,
                                       replication_slot=replication_slot_name,
                                       cdc_generator_queue_size=queue_size,
                                       poll_interval=1,
                                       tables=[{'schema': 'public',
                                                'excludePattern': '',
                                                'table': table_name} for table_name in table_names])
    if Version(sdc_builder.version) >= Version('4.2.0'):
        postgres_cdc_client.set_attributes(ssl_mode='DISABLED')
    if Version(sdc_builder.version) >= Version('5.1.0'):
        postgres_cdc_client.set_attributes(record_contents=record_contents,
                                           wal2json_format=wal2json_format)
    delay = pipeline_builder.add_stage('Delay')
    delay.delay_between_batches = 5000
    wiretap = pipeline_builder.add_wiretap()
    postgres_cdc_client >> delay >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    tables = [_create_table_in_database(table_name, database) for table_name in table_names]
    try:
        start_command = sdc_executor.start_pipeline(pipeline)
        connection = database.engine.connect()
        expected_operations_data = []
        for i in range(9):
            expected_operations_data += _insert(connection=connection,
                                                insert_rows=[{table_column_id: 1,
                                                              table_column_name: 'Dosaku'}],
                                                table=tables[i])

        def condition():
            pipeline_metrics = sdc_executor.get_pipeline_metrics(pipeline)
            queue_metrics = \
                pipeline_metrics.gauge('custom.PostgreSQLCDCClient_01.CDC Metrics.0.gauge').value
            output_records_from_origin = \
                pipeline_metrics.counter('stage.PostgreSQLCDCClient_01.outputRecords.counter').count
            if 0 < output_records_from_origin < len(expected_operations_data):
                assert 0 < queue_metrics['Queue Size'] <= queue_size
            assert queue_metrics['Queue Capacity'] == queue_size - queue_metrics['Queue Size']
            return output_records_from_origin >= len(expected_operations_data)

        def failure(timeout):
            pipeline_metrics = sdc_executor.get_pipeline_metrics(pipeline)
            output_records_from_origin = \
                pipeline_metrics.counter('stage.PostgreSQLCDCClient_01.outputRecords.counter').count
            raise Exception('Timed out after `{}` seconds waiting for Output record metrics `{}` to reach `{}` '
                            .format(timeout, output_records_from_origin, len(expected_operations_data)))

        wait_for_condition(condition=condition,  timeout=120, failure=failure)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 9, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)
        assert len(wiretap.output_records) == 9
    finally:
        if sdc_executor.get_pipeline_status(pipeline) == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        for t in tables:
            t.drop(database.engine)
        connection.close()


@database('postgresql')
@sdc_min_version('4.2.0')
@pytest.mark.parametrize('ssl_mode', ['REQUIRED',
                                      'VERIFY_CA',
                                      'VERIFY_FULL'])
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_postgres_cdc_ssl_enabled(sdc_builder,
                                  sdc_executor,
                                  database,
                                  ssl_mode,
                                  wal2json_format,
                                  record_contents):

    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgresSQL with CDC enabled.')

    if not database.ca_certificate:
        pytest.skip('Test only runs against PostgresSQL with SSL enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and wal2json_format == 'OPERATION':
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')


    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    postgres_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                       max_batch_size_in_records=1,
                                       remove_replication_slot_on_close=True,
                                       replication_slot=replication_slot_name,
                                       poll_interval=1,
                                       ssl_mode=ssl_mode)
    if ssl_mode != 'REQUIRED':
        postgres_cdc_client.set_attributes(ca_certificate_pem=database.ca_certificate_file_contents,
                                           server_certificate_pem=database.server_certificate_file_contents)
    if Version(sdc_builder.version) >= Version('5.1.0'):
        postgres_cdc_client.set_attributes(record_contents=record_contents,
                                           wal2json_format=wal2json_format)
    wiretap = pipeline_builder.add_wiretap()
    postgres_cdc_client >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = None
    try:
        sdc_executor.start_pipeline(pipeline)
        table = _create_table_in_database(table_name, database)
        connection = database.engine.connect()
        expected_operations_data = _insert(connection=connection, table=table)
        expected_operations_data += _update(connection=connection, table=table)
        expected_operations_data += _delete(connection=connection, table=table)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'data_batch_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        if record_contents == 'TRANSACTION':
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3, timeout_sec=300)
        else:
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 9, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)
        if record_contents == 'TRANSACTION':
            assert len(wiretap.output_records) == 3
        else:
            assert len(wiretap.output_records) == 9
        for record in wiretap.output_records:
            logger.info(f'Record :: {record}')
        if record_contents == 'TRANSACTION':
            operation_index = 0
            for record in wiretap.output_records:
                if record.get_field_data('/change'):
                    for i in range(3):
                        expected = expected_operations_data[operation_index]
                        assert expected.kind == record.get_field_data(f'/change[{i}]/kind')
                        assert expected.table == record.get_field_data(f'/change[{i}]/table')
                        if expected.kind != delete_kind:
                            assert expected.columnnames == record.get_field_data(f'/change[{i}]/columnnames')
                            assert expected.columnvalues == record.get_field_data(f'/change[{i}]/columnvalues')
                        if expected.kind != insert_kind:
                            assert expected.oldkeys.keynames == record.get_field_data(f'/change[{i}]/oldkeys/keynames')
                            assert expected.oldkeys.keyvalues == record.get_field_data(f'/change[{i}]/oldkeys/keyvalues')
                        operation_index += 1
            assert operation_index == len(expected_operations_data)
        else:
            expected_operations_data = transaction_data_to_operation_data(expected_operations_data,
                                                                          wal2json_format,
                                                                          record_contents)
            for record, expected in zip(wiretap.output_records, expected_operations_data):
                assert expected['operation'] == record.header.values['postgres.cdc.operation']
                assert expected['schema'] == record.header.values['postgres.cdc.schema']
                assert expected['table'] == record.header.values['postgres.cdc.table']
                if expected['operation'] != delete_kind.upper():
                    for expected_column_name, expected_column_value in expected['row_data'].items():
                        assert record.field[expected_column_name] == expected_column_value
                if expected['operation'] != insert_kind.upper():
                    for expected_column_name, expected_column_value in expected['primary_key_data'].items():
                        assert record.field[expected_column_name] == expected_column_value

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        #database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table is not None:
            table.drop(database.engine)
        connection.close()
