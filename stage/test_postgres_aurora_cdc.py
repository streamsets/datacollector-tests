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

import json
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
            connection.execute(
                table.update().where(table.c.id == row[table_column_id]).values(name=row[table_column_name]))
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
    for operation_name, operation_records in transaction_data.items():
        for operation in operation_records:
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


def transaction_data_to_operation_data_with_multiple_tables(transaction_data, wal2json_format, record_contents):
    wal2json_version = 2 if wal2json_format == 'OPERATION' and record_contents == 'OPERATION' else 1
    operations_data = []
    for operation_name, operation_records in transaction_data.items():
        for table_name, records in operation_records.items():
            for operation in records:
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


@database('postgresqlaurora')
@sdc_min_version('5.0.0')
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
        pytest.skip('Test only runs against Aurora PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and \
            (wal2json_format == 'OPERATION' or record_contents == 'OPERATION'):
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    sample_data = [dict(id=i, name=f'Takemiya{i}') for i in range(40)]

    table_name = f'stf_{get_random_string(string.ascii_lowercase, 20)}'
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
        aurora_postgresql_cdc_client = pipeline_builder.add_stage('Aurora PostgreSQL CDC Client')
        aurora_postgresql_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                                    max_batch_size_in_records=1,
                                                    poll_interval=poll_interval,
                                                    replication_slot=replication_slot,
                                                    tables=[{'schema': 'public',
                                                             'table': table_name,
                                                             'excludePattern': ''}])
        if Version(sdc_builder.version) >= Version('4.2.0'):
            aurora_postgresql_cdc_client.set_attributes(ssl_mode='DISABLED')
        if Version(sdc_builder.version) >= Version('5.1.0'):
            aurora_postgresql_cdc_client.set_attributes(record_contents=record_contents,
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
        aurora_postgresql_cdc_client >> [wiretap.destination, pipeline_finisher]
        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Starting pipeline for the first time ...')
        table.create(database.engine)
        sdc_executor.start_pipeline(pipeline)
        with database.engine.connect().execution_options(autocommit=True) as connection:
            for row in sample_data[:30]:
                connection.execute(table.insert(), row)
        sdc_executor.wait_for_pipeline_status(pipeline, 'FINISHED', timeout_sec=300)
        if record_contents == 'TRANSACTION':
            records = [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                       for record in wiretap.output_records]
            assert sorted(records, key=lambda d: str(d['id'])) == sorted(sample_data[:10], key=lambda d: str(d['id']))
        else:
            records = [{'id': record.field['id'], 'name': record.field['name']} for record in wiretap.output_records]
            assert sorted(records, key=lambda d: str(d['id'])) == sorted(sample_data[:10], key=lambda d: str(d['id']))
        wiretap.reset()

        logger.info('Starting pipeline for the second time ...')
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=300)
        if record_contents == 'TRANSACTION':
            records = [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                       for record in wiretap.output_records]
            assert sorted(records, key=lambda d: str(d['id'])) == sorted(sample_data[10:20], key=lambda d: str(d['id']))
        else:
            records = [{'id': record.field['id'], 'name': record.field['name']} for record in wiretap.output_records]
            assert sorted(records, key=lambda d: str(d['id'])) == sorted(sample_data[10:20], key=lambda d: str(d['id']))
        wiretap.reset()

        logger.info('Starting pipeline for the third time ...')
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=300)
        if record_contents == 'TRANSACTION':
            records = [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                       for record in wiretap.output_records]
            assert sorted(records, key=lambda d: str(d['id'])) == sorted(sample_data[20:30], key=lambda d: str(d['id']))
        else:
            records = [{'id': record.field['id'], 'name': record.field['name']} for record in wiretap.output_records]
            assert sorted(records, key=lambda d: str(d['id'])) == sorted(sample_data[20:30], key=lambda d: str(d['id']))
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
            records = [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                       for record in wiretap.output_records]
            assert sorted(records, key=lambda d: str(d['id'])) == sorted(sample_data[30:40], key=lambda d: str(d['id']))
        else:
            records = [{'id': record.field['id'], 'name': record.field['name']} for record in wiretap.output_records]
            assert sorted(records, key=lambda d: str(d['id'])) == sorted(sample_data[30:40], key=lambda d: str(d['id']))
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot)
        if table is not None:
            table.drop(database.engine)
        database.engine.connect().close()


@database('postgresqlaurora')
@sdc_min_version('5.0.0')
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
        pytest.skip('Test only runs against Aurora PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and \
            (wal2json_format == 'OPERATION' or record_contents == 'OPERATION'):
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    if start_from is 'LSN' and database.database_server_version.major < 10:
        pytest.skip('LSN test cannot be executed in versions PostgresSQL versions < 10.')

    sample_data_1 = [dict(id=f'1{i}', name=f'Takemiya_{i}') for i in range(20)]
    sample_data_2 = [dict(id=f'2{i}', name=f'Kobayashi_{i}') for i in range(20)]
    sample_data_3 = [dict(id=f'3{i}', name=f'Ishida_{i}') for i in range(20)]
    sample_data_4 = [dict(id=f'4{i}', name=f'Otake_{i}') for i in range(20)]

    table_name = f'stf_{get_random_string(string.ascii_lowercase, 20)}'
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
        aurora_postgresql_cdc_client = pipeline_builder.add_stage('Aurora PostgreSQL CDC Client')
        aurora_postgresql_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                                    max_batch_size_in_records=1,
                                                    replication_slot=replication_slot,
                                                    initial_change=start_from,
                                                    poll_interval=1,
                                                    tables=[{'schema': 'public',
                                                             'table': table_name,
                                                             'excludePattern': ''}])
        if start_from is 'DATE':
            aurora_postgresql_cdc_client.set_attributes(start_date=date.strftime('%m-%d-%Y %H:%M:%S'),
                                                        database_time_zone=timezone)
        else:
            aurora_postgresql_cdc_client.set_attributes(start_lsn=start_lsn)
        if Version(sdc_builder.version) >= Version('4.2.0'):
            aurora_postgresql_cdc_client.set_attributes(ssl_mode='DISABLED')
        if Version(sdc_builder.version) >= Version('5.1.0'):
            aurora_postgresql_cdc_client.set_attributes(record_contents=record_contents,
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
        aurora_postgresql_cdc_client >> [wiretap.destination, pipeline_finisher]
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
            records = [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                       for record in wiretap.output_records]
            assert sorted(records, key=lambda d: str(d['name'])) == sorted(expected_data, key=lambda d: str(d['name']))
        else:
            records = [{'id': record.field['id'], 'name': record.field['name']} for record in wiretap.output_records]
            assert sorted(records, key=lambda d: str(d['name'])) == sorted(expected_data, key=lambda d: str(d['name']))
        wiretap.reset()

        with database.engine.connect().execution_options(autocommit=True) as connection:
            for row in sample_data_3:
                connection.execute(table.insert(), row)
            sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_status(pipeline, 'FINISHED', timeout_sec=300)
        if record_contents == 'TRANSACTION':
            records = [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                       for record in wiretap.output_records]
            assert sorted(records, key=lambda d: str(d['name'])) == sorted(sample_data_3, key=lambda d: str(d['name']))
        else:
            records = [{'id': record.field['id'], 'name': record.field['name']}
                       for record in wiretap.output_records]
            assert sorted(records, key=lambda d: str(d['name'])) == sorted(sample_data_3, key=lambda d: str(d['name']))
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot)
        if table is not None:
            table.drop(database.engine)
        database.engine.connect().close()


@database('postgresqlaurora')
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_aurora_postgres_cdc_client_basic(sdc_builder,
                                          sdc_executor,
                                          database,
                                          wal2json_format,
                                          record_contents):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against Aurora PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and \
            (wal2json_format == 'OPERATION' or record_contents == 'OPERATION'):
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    table_name = f'stf_{get_random_string(string.ascii_lowercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    aurora_postgres_cdc_client = pipeline_builder.add_stage('Aurora PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    aurora_postgres_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                              max_batch_size_in_records=1,
                                              remove_replication_slot_on_close=True,
                                              poll_interval=1,
                                              replication_slot=replication_slot_name,
                                              tables=[{'schema': 'public',
                                                       'table': table_name,
                                                       'excludePattern': ''}])
    if Version(sdc_builder.version) >= Version('4.2.0'):
        aurora_postgres_cdc_client.set_attributes(ssl_mode='DISABLED')
    if Version(sdc_builder.version) >= Version('5.1.0'):
        aurora_postgres_cdc_client.set_attributes(record_contents=record_contents,
                                                  wal2json_format=wal2json_format)
    wiretap = pipeline_builder.add_wiretap()
    aurora_postgres_cdc_client >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = _create_table_in_database(table_name, database)
    try:
        sdc_executor.start_pipeline(pipeline)
        connection = database.engine.connect()
        expected_operations_data = {'insert': _insert(connection=connection, table=table),
                                    'update': _update(connection=connection, table=table),
                                    'delete': _delete(connection=connection, table=table)
                                    }

        if record_contents == 'TRANSACTION':
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3, timeout_sec=600)
        else:
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 9, timeout_sec=600)
        sdc_executor.stop_pipeline(pipeline)
        if record_contents == 'TRANSACTION':
            assert len(wiretap.output_records) == 3
        else:
            assert len(wiretap.output_records) == 9
        for record in wiretap.output_records:
            logger.info(f'Record :: {record}')
        if record_contents == 'TRANSACTION':
            for record in wiretap.output_records:
                if record.get_field_data('/change'):
                    operation = record.get_field_data(f'/change[0]/kind')
                    expected_records = expected_operations_data[operation]
                    for i in range(3):
                        expected = expected_records[i]
                        assert expected.kind == operation
                        assert expected.table == record.get_field_data(f'/change[{i}]/table')
                        if expected.kind != delete_kind:
                            assert expected.columnnames == record.get_field_data(f'/change[{i}]/columnnames')
                            assert expected.columnvalues == record.get_field_data(f'/change[{i}]/columnvalues')
                        if expected.kind != insert_kind:
                            assert expected.oldkeys.keynames == record.get_field_data(f'/change[{i}]/oldkeys/keynames')
                            assert expected.oldkeys.keyvalues == record.get_field_data(
                                f'/change[{i}]/oldkeys/keyvalues')
        else:
            expected_operations_data = transaction_data_to_operation_data(expected_operations_data,
                                                                          wal2json_format,
                                                                          record_contents)

            records = [{"table": r.header.values["postgres.cdc.table"], "row_data": r.field,
                        "operation": r.header.values["postgres.cdc.operation"],
                        "schema": r.header.values["postgres.cdc.schema"]} for r in wiretap.output_records]

            records = sorted(records, key=lambda x: f"{x['table']}_{x['operation']}_{x['row_data']['id']}")
            expected_operations_data = sorted(expected_operations_data,
                                              key=lambda
                                                  x: f"{x['table']}_{x['operation']}_{x['row_data' if x['operation'] not in [delete_kind.upper(), 'D'] else 'primary_key_data']['id']}")


            for record, expected in zip(records, expected_operations_data):
                assert expected['operation'] == record['operation']
                assert expected['schema'] == record['schema']
                assert expected['table'] == record['table']
                if expected['operation'] != delete_kind.upper():
                    for expected_column_name, expected_column_value in expected['row_data'].items():
                        assert record['row_data'][expected_column_name] == expected_column_value
                if expected['operation'] != insert_kind.upper():
                    for expected_column_name, expected_column_value in expected['primary_key_data'].items():
                        assert record['row_data'][expected_column_name] == expected_column_value

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table is not None:
            table.drop(database.engine)
        connection.close()


@database('postgresqlaurora')
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_aurora_postgres_cdc_max_poll_attempts(sdc_builder,
                                               sdc_executor,
                                               database,
                                               wal2json_format,
                                               record_contents):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against Aurora PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and \
            (wal2json_format == 'OPERATION' or record_contents == 'OPERATION'):
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    table_name = f'stf_{get_random_string(string.ascii_lowercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    aurora_postgres_cdc_client = pipeline_builder.add_stage('Aurora PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    aurora_postgres_cdc_client.set_attributes(batch_wait_time_in_ms=30000,
                                              max_batch_size_in_records=33,
                                              remove_replication_slot_on_close=True,
                                              poll_interval=1,
                                              replication_slot=replication_slot_name,
                                              tables=[{'schema': 'public',
                                                       'table': table_name,
                                                       'excludePattern': ''}])
    if Version(sdc_builder.version) >= Version('4.2.0'):
        aurora_postgres_cdc_client.set_attributes(ssl_mode='DISABLED')
    if Version(sdc_builder.version) >= Version('5.1.0'):
        aurora_postgres_cdc_client.set_attributes(record_contents=record_contents,
                                                  wal2json_format=wal2json_format)
    wiretap = pipeline_builder.add_wiretap()
    aurora_postgres_cdc_client >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = _create_table_in_database(table_name, database)
    try:
        sdc_executor.start_pipeline(pipeline)
        connection = database.engine.connect()
        expected_operations_data = {'insert': _insert(connection=connection, table=table)}
        sdc_executor.wait_for_pipeline_metric(pipeline, 'data_batch_count', 1, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        if record_contents == 'TRANSACTION':
            assert len(wiretap.output_records) == 1
        else:
            assert len(wiretap.output_records) == 3
        if record_contents == 'TRANSACTION':
            operation_count = 0
            for record in wiretap.output_records:
                if record.get_field_data('/change'):
                    operation = record.get_field_data(f'/change[0]/kind')
                    expected_records = expected_operations_data[operation]
                    for i in range(3):
                        expected = expected_records[i]
                        assert expected.kind == record.get_field_data(f'/change[{i}]/kind')
                        assert expected.table == record.get_field_data(f'/change[{i}]/table')
                        if expected.kind != delete_kind:
                            assert expected.columnnames == record.get_field_data(f'/change[{i}]/columnnames')
                            assert expected.columnvalues == record.get_field_data(f'/change[{i}]/columnvalues')
                        if expected.kind != insert_kind:
                            assert expected.oldkeys.keynames == record.get_field_data(f'/change[{i}]/oldkeys/keynames')
                            assert expected.oldkeys.keyvalues == record.get_field_data(
                                f'/change[{i}]/oldkeys/keyvalues')
                    operation_count += 1
            assert operation_count == len(expected_operations_data)
        else:
            expected_operations_data = transaction_data_to_operation_data(expected_operations_data,
                                                                          wal2json_format,
                                                                          record_contents)

            records = [{"table": r.header.values["postgres.cdc.table"], "row_data": r.field,
                        "operation": r.header.values["postgres.cdc.operation"],
                        "schema": r.header.values["postgres.cdc.schema"]} for r in wiretap.output_records]

            records = sorted(records, key=lambda x: f"{x['table']}_{x['operation']}_{x['row_data']['id']}")
            expected_operations_data = sorted(expected_operations_data,
                                              key=lambda
                                                  x: f"{x['table']}_{x['operation']}_{x['row_data' if x['operation'] not in [delete_kind.upper(), 'D'] else 'primary_key_data']['id']}")


            for record, expected in zip(records, expected_operations_data):
                assert expected['operation'] == record['operation']
                assert expected['schema'] == record['schema']
                assert expected['table'] == record['table']
                if expected['operation'] != delete_kind.upper():
                    for expected_column_name, expected_column_value in expected['row_data'].items():
                        assert record['row_data'][expected_column_name] == expected_column_value
                if expected['operation'] != insert_kind.upper():
                    for expected_column_name, expected_column_value in expected['primary_key_data'].items():
                        assert record['row_data'][expected_column_name] == expected_column_value

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table is not None:
            table.drop(database.engine)
        connection.close()


@database('postgresqlaurora')
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_aurora_postgres_cdc_client_filtering_table(sdc_builder,
                                                    sdc_executor,
                                                    database,
                                                    wal2json_format,
                                                    record_contents):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against Aurora PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and \
            (wal2json_format == 'OPERATION' or record_contents == 'OPERATION'):
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    table_name_allow = f'stf_{get_random_string(string.ascii_lowercase, 20)}'
    table_name_deny = f'stf_{get_random_string(string.ascii_lowercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    aurora_postgres_cdc_client = pipeline_builder.add_stage('Aurora PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    aurora_postgres_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                              max_batch_size_in_records=1,
                                              remove_replication_slot_on_close=True,
                                              poll_interval=1,
                                              replication_slot=replication_slot_name,
                                              tables=[{'schema': 'public',
                                                       'excludePattern': table_name_deny,
                                                       'table': table_name_allow}])
    if Version(sdc_builder.version) >= Version('4.2.0'):
        aurora_postgres_cdc_client.set_attributes(ssl_mode='DISABLED')
    if Version(sdc_builder.version) >= Version('5.1.0'):
        aurora_postgres_cdc_client.set_attributes(record_contents=record_contents,
                                                  wal2json_format=wal2json_format)

    wiretap = pipeline_builder.add_wiretap()
    aurora_postgres_cdc_client >> wiretap.destination
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
        expected_operations_data = {'insert': _insert(connection=connection, table=table_allow),
                                    'update': _update(connection=connection, table=table_allow),
                                    'delete': _delete(connection=connection, table=table_allow)
                                    }

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
            operation_count = 0
            for record in wiretap.output_records:
                if record.get_field_data('/change'):
                    operation = record.get_field_data(f'/change[0]/kind')
                    expected_records = expected_operations_data[operation]
                    for i in range(3):
                        expected = expected_records[i]
                        assert expected.kind == operation
                        assert expected.kind == record.get_field_data(f'/change[{i}]/kind')
                        assert expected.table == record.get_field_data(f'/change[{i}]/table')
                        if expected.kind != delete_kind:
                            assert expected.columnnames == record.get_field_data(f'/change[{i}]/columnnames')
                            assert expected.columnvalues == record.get_field_data(f'/change[{i}]/columnvalues')
                        if expected.kind != insert_kind:
                            assert expected.oldkeys.keynames == record.get_field_data(f'/change[{i}]/oldkeys/keynames')
                            assert expected.oldkeys.keyvalues == record.get_field_data(f'/change[{i}]/oldkeys/keyvalues')
                    operation_count += 1
            assert operation_count == len(expected_operations_data)
        else:
            expected_operations_data = transaction_data_to_operation_data(expected_operations_data,
                                                                          wal2json_format,
                                                                          record_contents)

            records = [{"table": r.header.values["postgres.cdc.table"], "row_data": r.field,
                        "operation": r.header.values["postgres.cdc.operation"],
                        "schema": r.header.values["postgres.cdc.schema"]} for r in wiretap.output_records]

            records = sorted(records, key=lambda x: f"{x['table']}_{x['operation']}_{x['row_data']['id']}")
            expected_operations_data = sorted(expected_operations_data,
                                              key=lambda
                                                  x: f"{x['table']}_{x['operation']}_{x['row_data' if x['operation'] not in [delete_kind.upper(), 'D'] else 'primary_key_data']['id']}")

            for record, expected in zip(records, expected_operations_data):
                assert expected['operation'] == record['operation']
                assert expected['schema'] == record['schema']
                assert expected['table'] == record['table']
                if expected['operation'] != delete_kind.upper():
                    for expected_column_name, expected_column_value in expected['row_data'].items():
                        assert record['row_data'][expected_column_name] == expected_column_value
                if expected['operation'] != insert_kind.upper():
                    for expected_column_name, expected_column_value in expected['primary_key_data'].items():
                        assert record['row_data'][expected_column_name] == expected_column_value

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table_allow is not None:
            table_allow.drop(database.engine)
        if table_deny is not None:
            table_deny.drop(database.engine)
        connection.close()


@database('postgresqlaurora')
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_aurora_postgres_cdc_client_remove_replication_slot(sdc_builder,
                                                            sdc_executor,
                                                            database,
                                                            wal2json_format,
                                                            record_contents):
    if database.database_server_version < databases.EARLIEST_POSTGRESQL_VERSION_WITH_ACTIVE_PID:
        pytest.skip('Test only runs against Aurora PostgresSQL version >= '
                    f"{'.'.join(str(item) for item in databases.EARLIEST_POSTGRESQL_VERSION_WITH_ACTIVE_PID)}")

    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against Aurora PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and \
            (wal2json_format == 'OPERATION' or record_contents == 'OPERATION'):
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    table_name = f'stf_{get_random_string(string.ascii_lowercase, 20)}'
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    aurora_postgres_cdc_client = pipeline_builder.add_stage('Aurora PostgreSQL CDC Client')
    aurora_postgres_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                              max_batch_size_in_records=1,
                                              remove_replication_slot_on_close=True,
                                              poll_interval=1,
                                              replication_slot=replication_slot_name,
                                              tables=[{'schema': 'public',
                                                       'table': table_name,
                                                       'excludePattern': ''}]
                                              )
    if Version(sdc_builder.version) >= Version('4.2.0'):
        aurora_postgres_cdc_client.set_attributes(ssl_mode='DISABLED')
    if Version(sdc_builder.version) >= Version('5.1.0'):
        aurora_postgres_cdc_client.set_attributes(record_contents=record_contents,
                                                  wal2json_format=wal2json_format)
    wiretap = pipeline_builder.add_wiretap()
    aurora_postgres_cdc_client >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = _create_table_in_database(table_name, database)
    try:
        sdc_executor.start_pipeline(pipeline)
        connection = database.engine.connect()
        expected_operations_data = {'insert': _insert(connection=connection, table=table),
                                    'update': _update(connection=connection, table=table),
                                    'delete': _delete(connection=connection, table=table)
                                    }
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


@database('postgresqlaurora')
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('batch_size', [1,
                                        10,
                                        100,
                                        1000])
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_aurora_postgres_cdc_client_multiple_concurrent_operations(sdc_builder,
                                                                   sdc_executor,
                                                                   database,
                                                                   batch_size,
                                                                   wal2json_format,
                                                                   record_contents):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against Aurora PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and \
            (wal2json_format == 'OPERATION' or record_contents == 'OPERATION'):
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    table_name = f'stf_{get_random_string(string.ascii_lowercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    aurora_postgres_cdc_client = pipeline_builder.add_stage('Aurora PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    if record_contents == 'TRANSACTION':
        max_batch_size_in_records = batch_size
        batch_wait_time_in_ms = 3000
        timeout_sec = 120
    else:
        max_batch_size_in_records = 10 * batch_size
        batch_wait_time_in_ms = 30000
        timeout_sec = 300
    aurora_postgres_cdc_client.set_attributes(batch_wait_time_in_ms=batch_wait_time_in_ms,
                                              max_batch_size_in_records=max_batch_size_in_records,
                                              remove_replication_slot_on_close=False,
                                              poll_interval=1,
                                              replication_slot=replication_slot_name,
                                              tables=[{'schema': 'public',
                                                       'table': table_name,
                                                       'excludePattern': ''}])
    if Version(sdc_builder.version) >= Version('4.2.0'):
        aurora_postgres_cdc_client.set_attributes(ssl_mode='DISABLED')
    if Version(sdc_builder.version) >= Version('5.1.0'):
        aurora_postgres_cdc_client.set_attributes(record_contents=record_contents,
                                                  wal2json_format=wal2json_format)
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    if record_contents == 'TRANSACTION':
        precondition = "${record:value('/change[0]/columnvalues[0]') == -1}"
    else:
        precondition = "${record:value('/id') == -1}"
    pipeline_finisher.set_attributes(preconditions=[precondition])
    aurora_postgres_cdc_client >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = _create_table_in_database(table_name, database)
    try:
        pipeline_cmd = sdc_executor.start_pipeline(pipeline)
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
                    output_values.append(
                        {'type': 'delete', 'value': record.get_field_data('/change[0]/oldkeys/keyvalues')})
                if record.get_field_data('/change[0]/kind') == 'insert':
                    output_values.append({'type': 'insert', 'value': record.get_field_data('/change[0]/columnvalues')})
                if record.get_field_data('/change[0]/kind') == 'update':
                    output_values.append({'type': 'update', 'value': record.get_field_data('/change[0]/columnvalues')})
            output_sorted_values = sorted(output_values, key=lambda key: f'{key["value"][0]}|{key["type"]}')
        else:
            for record in wiretap.output_records:
                if record.header.values['postgres.cdc.operation'] == 'DELETE' or record.header.values[
                    'postgres.cdc.operation'] == 'D':
                    output_values.append({'type': 'delete', 'value': record.field['id']})
                if record.header.values['postgres.cdc.operation'] == 'INSERT' or record.header.values[
                    'postgres.cdc.operation'] == 'I':
                    output_values.append({'type': 'insert', 'value': record.field['id']})
                elif record.header.values['postgres.cdc.operation'] == 'UPDATE' or record.header.values[
                    'postgres.cdc.operation'] == 'U':
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


@database('postgresqlaurora')
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_aurora_postgres_cdc_client_filtering_multiple_tables(sdc_builder,
                                                              sdc_executor,
                                                              database,
                                                              wal2json_format,
                                                              record_contents):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against Aurora PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and \
            (wal2json_format == 'OPERATION' or record_contents == 'OPERATION'):
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    table_name = [f'stf_{get_random_string(string.ascii_lowercase, 20)}' for _ in range(4)]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    aurora_postgres_cdc_client = pipeline_builder.add_stage('Aurora PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    aurora_postgres_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                              max_batch_size_in_records=1,
                                              remove_replication_slot_on_close=True,
                                              replication_slot=replication_slot_name,
                                              poll_interval=1,
                                              tables=[{'schema': 'public',
                                                       'excludePattern': '',
                                                       'table': table_name[i]} for i in range(3)])
    if Version(sdc_builder.version) >= Version('4.2.0'):
        aurora_postgres_cdc_client.set_attributes(ssl_mode='DISABLED')
    if Version(sdc_builder.version) >= Version('5.1.0'):
        aurora_postgres_cdc_client.set_attributes(record_contents=record_contents,
                                                  wal2json_format=wal2json_format)
    wiretap = pipeline_builder.add_wiretap()
    aurora_postgres_cdc_client >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = []
    try:
        table = [_create_table_in_database(name, database) for name in table_name]
        sdc_executor.start_pipeline(pipeline)
        with database.engine.connect().execution_options(autocommit=False) as connection:
            expected_insert_data = {}
            expected_update_data = {}
            expected_delete_data = {}
            for i in range(3):
                expected_insert_data[table_name[i]] = _insert(connection=connection, table=table[i], create_txn=True)
                time.sleep(1)
                expected_update_data[table_name[i]] = _update(connection=connection, table=table[i])
                time.sleep(1)
                expected_delete_data[table_name[i]] = _delete(connection=connection, table=table[i])
                time.sleep(1)
            expected_operations_data = {'insert': expected_insert_data,
                                        'update': expected_update_data,
                                        'delete': expected_delete_data}

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
            for record in wiretap.output_records:
                if record.get_field_data('/change'):
                    operation = record.get_field_data(f'/change[0]/kind')
                    t_name = record.get_field_data(f'/change[0]/table').value
                    expected_records = expected_operations_data[operation][t_name]
                    for i in range(3):
                        expected = expected_records[i]
                        assert expected.kind == operation
                        assert expected.kind == record.get_field_data(f'/change[{i}]/kind')
                        assert expected.table == record.get_field_data(f'/change[{i}]/table')
                        if expected.kind != delete_kind:
                            assert expected.columnnames == record.get_field_data(f'/change[{i}]/columnnames')
                            assert expected.columnvalues == record.get_field_data(f'/change[{i}]/columnvalues')
                        if expected.kind != insert_kind:
                            assert expected.oldkeys.keynames == record.get_field_data(f'/change[{i}]/oldkeys/keynames')
                            assert expected.oldkeys.keyvalues == record.get_field_data(
                                f'/change[{i}]/oldkeys/keyvalues')
        else:
            expected_operations_data = transaction_data_to_operation_data_with_multiple_tables(expected_operations_data,
                                                                                               wal2json_format,
                                                                                               record_contents)

            records = [{"table": r.header.values["postgres.cdc.table"], "row_data": r.field,
                        "operation": r.header.values["postgres.cdc.operation"],
                        "schema": r.header.values["postgres.cdc.schema"]} for r in wiretap.output_records]

            records = sorted(records, key=lambda x: f"{x['table']}_{x['operation']}_{x['row_data']['id']}")
            expected_operations_data = sorted(expected_operations_data,
                                              key=lambda
                                                  x: f"{x['table']}_{x['operation']}_{x['row_data' if x['operation'] not in [delete_kind.upper(), 'D'] else 'primary_key_data']['id']}")

            for record, expected in zip(records, expected_operations_data):
                assert expected['operation'] == record['operation']
                assert expected['schema'] == record['schema']
                assert expected['table'] == record['table']
                if expected['operation'] != delete_kind.upper():
                    for expected_column_name, expected_column_value in expected['row_data'].items():
                        assert record['row_data'][expected_column_name] == expected_column_value
                if expected['operation'] != insert_kind.upper():
                    for expected_column_name, expected_column_value in expected['primary_key_data'].items():
                        assert record['row_data'][expected_column_name] == expected_column_value

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        for t in table:
            t.drop(database.engine)
        database.engine.connect().close()


@database('postgresqlaurora')
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_aurora_postgres_cdc_wal_sender_status_metrics(sdc_builder,
                                                       sdc_executor,
                                                       database,
                                                       wal2json_format,
                                                       record_contents):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against Aurora PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and \
            (wal2json_format == 'OPERATION' or record_contents == 'OPERATION'):
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    table_name = f'stf_{get_random_string(string.ascii_lowercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    aurora_postgres_cdc_client = pipeline_builder.add_stage('Aurora PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    aurora_postgres_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                              max_batch_size_in_records=1,
                                              remove_replication_slot_on_close=True,
                                              replication_slot=replication_slot_name,
                                              poll_interval=1,
                                              tables=[{'schema': 'public',
                                                       'excludePattern': '',
                                                       'table': table_name}])
    if Version(sdc_builder.version) >= Version('4.2.0'):
        aurora_postgres_cdc_client.set_attributes(ssl_mode='DISABLED')
    if Version(sdc_builder.version) >= Version('5.1.0'):
        aurora_postgres_cdc_client.set_attributes(record_contents=record_contents,
                                                  wal2json_format=wal2json_format)
    trash = pipeline_builder.add_stage('Trash')
    aurora_postgres_cdc_client >> trash
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)
    table = _create_table_in_database(table_name, database)
    try:
        sdc_executor.start_pipeline(pipeline)
        connection = database.engine.connect()
        expected_operations_data = {'insert': _insert(connection=connection, table=table)}
        time.sleep(1)
        expected_operations_data['update'] = _update(connection=connection, table=table)
        time.sleep(1)
        expected_operations_data['delete'] = _delete(connection=connection, table=table)
        if record_contents == 'TRANSACTION':
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3, timeout_sec=300)
        else:
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 9, timeout_sec=300)
        wal_sender_status_from_db = _get_wal_sender_status(connection)
        assert wal_sender_status_from_db is not None
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        wal_sender_status_from_metrics = \
            history.latest.metrics.gauge('custom.AuroraPostgreSQLCDCClient_01.Wal Sender Status.0.gauge').value
        assert all([k not in wal_sender_status_from_metrics for k in wal_sender_columns_blacklist])
        assert ({k: str(v) for k, v in wal_sender_status_from_metrics.items() if k in wal_sender_columns_whitelist} ==
                {k: str(v) for k, v in wal_sender_status_from_db.items() if k in wal_sender_columns_whitelist})
    finally:
        if sdc_executor.get_pipeline_status(pipeline) == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        table.drop(database.engine)
        connection.close()


@database('postgresqlaurora')
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_aurora_postgres_cdc_queue_buffering_metrics(sdc_builder,
                                                     sdc_executor,
                                                     database,
                                                     wal2json_format,
                                                     record_contents):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against Aurora PostgresSQL with CDC enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and \
            (wal2json_format == 'OPERATION' or record_contents == 'OPERATION'):
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    table_names = [f'stf_{get_random_string(string.ascii_lowercase, 20)}' for _ in range(9)]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    aurora_postgres_cdc_client = pipeline_builder.add_stage('Aurora PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    queue_size = 6
    aurora_postgres_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                              max_batch_size_in_records=1,
                                              remove_replication_slot_on_close=True,
                                              replication_slot=replication_slot_name,
                                              cdc_generator_queue_size=queue_size,
                                              poll_interval=1,
                                              tables=[{'schema': 'public',
                                                       'excludePattern': '',
                                                       'table': table_name} for table_name in table_names])
    if Version(sdc_builder.version) >= Version('4.2.0'):
        aurora_postgres_cdc_client.set_attributes(ssl_mode='DISABLED')
    if Version(sdc_builder.version) >= Version('5.1.0'):
        aurora_postgres_cdc_client.set_attributes(record_contents=record_contents,
                                                  wal2json_format=wal2json_format)
    delay = pipeline_builder.add_stage('Delay')
    delay.delay_between_batches = 5000
    wiretap = pipeline_builder.add_wiretap()
    aurora_postgres_cdc_client >> delay >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    tables = [_create_table_in_database(table_name, database) for table_name in table_names]
    try:
        sdc_executor.start_pipeline(pipeline)
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
                pipeline_metrics.gauge('custom.AuroraPostgreSQLCDCClient_01.CDC Metrics.0.gauge').value
            output_records_from_origin = \
                pipeline_metrics.counter('stage.AuroraPostgreSQLCDCClient_01.outputRecords.counter').count
            if 0 < output_records_from_origin < len(expected_operations_data):
                assert 0 < queue_metrics['Queue Size'] <= queue_size
            assert queue_metrics['Queue Capacity'] == queue_size - queue_metrics['Queue Size']
            return output_records_from_origin >= len(expected_operations_data)

        def failure(timeout):
            pipeline_metrics = sdc_executor.get_pipeline_metrics(pipeline)
            output_records_from_origin = \
                pipeline_metrics.counter('stage.AuroraPostgreSQLCDCClient_01.outputRecords.counter').count
            raise Exception('Timed out after `{}` seconds waiting for Output record metrics `{}` to reach `{}` '
                            .format(timeout, output_records_from_origin, len(expected_operations_data)))

        wait_for_condition(condition=condition, timeout=120, failure=failure)
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


@database('postgresqlaurora')
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('ssl_mode', ['REQUIRED',
                                      'VERIFY_CA',
                                      'VERIFY_FULL'])
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'TRANSACTION'),
                                                              ('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_aurora_postgres_cdc_ssl_enabled(sdc_builder,
                                         sdc_executor,
                                         database,
                                         ssl_mode,
                                         wal2json_format,
                                         record_contents):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against Aurora PostgresSQL with CDC enabled.')

    if not database.ca_certificate:
        pytest.skip('Test only runs against Aurora PostgresSQL with SSL enabled.')

    if Version(sdc_builder.version) < Version('5.1.0') and \
            (wal2json_format == 'OPERATION' or record_contents == 'OPERATION'):
        pytest.skip('Record contents OPERATION is only supported in SDC versions >= 5.1.0')

    table_name = f'stf_{get_random_string(string.ascii_lowercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    aurora_postgres_cdc_client = pipeline_builder.add_stage('Aurora PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    aurora_postgres_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                              max_batch_size_in_records=1,
                                              remove_replication_slot_on_close=True,
                                              replication_slot=replication_slot_name,
                                              poll_interval=1,
                                              ssl_mode=ssl_mode,
                                              tables=[{'schema': 'public',
                                                       'table': table_name,
                                                       'excludePattern': ''}])
    if ssl_mode != 'REQUIRED':
        aurora_postgres_cdc_client.set_attributes(ca_certificate_pem=database.ca_certificate_file_contents,
                                                  server_certificate_pem=database.server_certificate_file_contents)
    if Version(sdc_builder.version) >= Version('5.1.0'):
        aurora_postgres_cdc_client.set_attributes(record_contents=record_contents,
                                                  wal2json_format=wal2json_format)
    wiretap = pipeline_builder.add_wiretap()
    aurora_postgres_cdc_client >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = _create_table_in_database(table_name, database)
    try:
        sdc_executor.start_pipeline(pipeline)
        connection = database.engine.connect()
        expected_operations_data = {'insert': _insert(connection=connection, table=table),
                                    'update': _update(connection=connection, table=table),
                                    'delete': _delete(connection=connection, table=table)
                                    }

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
            operation_count = 0
            for record in wiretap.output_records:
                if record.get_field_data('/change'):
                    operation = record.get_field_data(f'/change[0]/kind')
                    expected_records = expected_operations_data[operation]
                    for i in range(3):
                        expected = expected_records[i]
                        assert expected.kind == operation
                        assert expected.table == record.get_field_data(f'/change[{i}]/table')
                        if expected.kind != delete_kind:
                            assert expected.columnnames == record.get_field_data(f'/change[{i}]/columnnames')
                            assert expected.columnvalues == record.get_field_data(f'/change[{i}]/columnvalues')
                        if expected.kind != insert_kind:
                            assert expected.oldkeys.keynames == record.get_field_data(f'/change[{i}]/oldkeys/keynames')
                            assert expected.oldkeys.keyvalues == record.get_field_data(
                                f'/change[{i}]/oldkeys/keyvalues')
                    operation_count += 1
            assert operation_count == len(expected_operations_data)
        else:
            expected_operations_data = transaction_data_to_operation_data(expected_operations_data,
                                                                          wal2json_format,
                                                                          record_contents)

            records = [{"table": r.header.values["postgres.cdc.table"], "row_data": r.field,
                        "operation": r.header.values["postgres.cdc.operation"],
                        "schema": r.header.values["postgres.cdc.schema"]} for r in wiretap.output_records]

            records = sorted(records, key=lambda x: f"{x['table']}_{x['operation']}_{x['row_data']['id']}")
            expected_operations_data = sorted(expected_operations_data,
                                              key=lambda
                                                  x: f"{x['table']}_{x['operation']}_{x['row_data' if x['operation'] not in [delete_kind.upper(), 'D'] else 'primary_key_data']['id']}")

            for record, expected in zip(records, expected_operations_data):
                assert expected['operation'] == record['operation']
                assert expected['schema'] == record['schema']
                assert expected['table'] == record['table']
                if expected['operation'] != delete_kind.upper():
                    for expected_column_name, expected_column_value in expected['row_data'].items():
                        assert record['row_data'][expected_column_name] == expected_column_value
                if expected['operation'] != insert_kind.upper():
                    for expected_column_name, expected_column_value in expected['primary_key_data'].items():
                        assert record['row_data'][expected_column_name] == expected_column_value

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        # database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table is not None:
            table.drop(database.engine)
        connection.close()


@database('postgresqlaurora')
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
@pytest.mark.parametrize('parse_datetimes', [True,
                                             False])
def test_aurora_postgres_cdc_client_primary_keys_metadata_headers(sdc_builder,
                                                                  sdc_executor,
                                                                  database,
                                                                  wal2json_format,
                                                                  record_contents,
                                                                  parse_datetimes):
    pipeline = None

    try:

        database_connection = database.engine.connect()

        replication_slot_name = get_random_string(string.ascii_lowercase, 10)

        table_name = f'stf_{get_random_string(string.ascii_lowercase, 16)}'

        pipeline_builder = sdc_builder.get_pipeline_builder()
        aurora_postgres_cdc_client = pipeline_builder.add_stage('Aurora PostgreSQL CDC Client')
        aurora_postgres_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                                  max_batch_size_in_records=1,
                                                  remove_replication_slot_on_close=True,
                                                  poll_interval=1,
                                                  replication_slot=replication_slot_name,
                                                  ssl_mode='DISABLED',
                                                  record_contents=record_contents,
                                                  wal2json_format=wal2json_format,
                                                  parse_datetimes=parse_datetimes,
                                                  tables=[{'schema': 'public',
                                                           'table': table_name,
                                                           'excludePattern': ''}])
        wiretap = pipeline_builder.add_wiretap()
        aurora_postgres_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        database_connection.execute(f"""create table {table_name}
                                                     (my_bigint                   bigint,
                                                      my_bigserial                bigserial,
                                                      my_bit                      bit(2),
                                                      my_bit_varying              bit varying(2),
                                                      my_boolean                  boolean,
                                                      my_bytea                    bytea,
                                                      my_character                character(32),
                                                      my_character_varying        character varying(32),
                                                      my_date                     date,
                                                      my_double_precision         double precision,
                                                      my_integer                  integer,
                                                      my_numeric                  numeric(6, 3),
                                                      my_real                     real,
                                                      my_smallint                 smallint,
                                                      my_text                     text,
                                                      my_time                     time(3),
                                                      my_time_with_time_zone      time(3) with time zone,
                                                      my_timestamp                timestamp(3),
                                                      my_timestamp_with_time_zone timestamp(3) with time zone,  
                                         primary key (my_bigint,                                                                                                          
                                                      my_bigserial,
                                                      my_bit,
                                                      my_bit_varying,
                                                      my_boolean,
                                                      my_bytea,
                                                      my_character,
                                                      my_character_varying,
                                                      my_date,
                                                      my_double_precision,
                                                      my_integer,
                                                      my_numeric,
                                                      my_real,
                                                      my_smallint,
                                                      my_text,
                                                      my_time,
                                                      my_time_with_time_zone,
                                                      my_timestamp,
                                                      my_timestamp_with_time_zone))""")

        sdc_executor.start_pipeline(pipeline)

        transaction = database_connection.begin()
        database_connection.execute(f"""insert into {table_name}
                                                    (my_bigint,
                                                     my_bigserial,                                                                                                          
                                                     my_bit,
                                                     my_bit_varying,
                                                     my_boolean,
                                                     my_bytea,
                                                     my_character,
                                                     my_character_varying,
                                                     my_date,
                                                     my_double_precision,
                                                     my_integer,
                                                     my_numeric,
                                                     my_real,
                                                     my_smallint,
                                                     my_text,
                                                     my_time,
                                                     my_time_with_time_zone,
                                                     my_timestamp,
                                                     my_timestamp_with_time_zone)
                                              values (0,
                                                      '0',
                                                      B'00',
                                                      '0',
                                                      true,
                                                      '0',
                                                      '0',
                                                      '0',
                                                      '2000-01-01',
                                                      1.1,
                                                      0,
                                                      1.1,
                                                      1.1,
                                                      0,
                                                      '0',
                                                      '01:01:01.111',
                                                      '01:01:01.111 UTC',
                                                      current_timestamp(3),
                                                      current_timestamp(3))""")
        transaction.commit()

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)
        assert len(wiretap.output_records) == 1

        primary_key_specification_expected = \
            '''{"my_bigint":                   {"currency": false, "datatype": "BIGINT",    "precision": 19,         "scale": 0,  "signed": true,  "size": 20,         "type": -5}, 
                "my_bigserial":                {"currency": false, "datatype": "BIGINT",    "precision": 19,         "scale": 0,  "signed": true,  "size": 20,         "type": -5}, 
                "my_bit":                      {"currency": false, "datatype": "BIT",       "precision": 2,          "scale": 0,  "signed": false, "size": 2,          "type": -7}, 
                "my_bit_varying":              {"currency": false, "datatype": "OTHER",     "precision": 2,          "scale": 0,  "signed": false, "size": 2,          "type": 1111}, 
                "my_boolean":                  {"currency": false, "datatype": "BIT",       "precision": 1,          "scale": 0,  "signed": false, "size": 1,          "type": -7}, 
                "my_bytea":                    {"currency": false, "datatype": "BINARY",    "precision": 2147483647, "scale": 0,  "signed": false, "size": 2147483647, "type": -2}, 
                "my_character":                {"currency": false, "datatype": "CHAR",      "precision": 32,         "scale": 0,  "signed": false, "size": 32,         "type": 1}, 
                "my_character_varying":        {"currency": false, "datatype": "VARCHAR",   "precision": 32,         "scale": 0,  "signed": false, "size": 32,         "type": 12}, 
                "my_date":                     {"currency": false, "datatype": "DATE",      "precision": 13,         "scale": 0,  "signed": false, "size": 13,         "type": 91}, 
                "my_double_precision":         {"currency": false, "datatype": "DOUBLE",    "precision": 17,         "scale": 17, "signed": true,  "size": 25,         "type": 8}, 
                "my_integer":                  {"currency": false, "datatype": "INTEGER",   "precision": 10,         "scale": 0,  "signed": true,  "size": 11,         "type": 4}, 
                "my_numeric":                  {"currency": false, "datatype": "NUMERIC",   "precision": 6,          "scale": 3,  "signed": true,  "size": 8,          "type": 2}, 
                "my_real":                     {"currency": false, "datatype": "REAL",      "precision": 8,          "scale": 8,  "signed": true,  "size": 15,         "type": 7}, 
                "my_smallint":                 {"currency": false, "datatype": "SMALLINT",  "precision": 5,          "scale": 0,  "signed": true,  "size": 6,          "type": 5}, 
                "my_text":                     {"currency": false, "datatype": "VARCHAR",   "precision": 2147483647, "scale": 0,  "signed": false, "size": 2147483647, "type": 12}, 
                "my_time":                     {"currency": false, "datatype": "TIME",      "precision": 12,         "scale": 3,  "signed": false, "size": 12,         "type": 92}, 
                "my_time_with_time_zone":      {"currency": false, "datatype": "TIME",      "precision": 18,         "scale": 3,  "signed": false, "size": 18,         "type": 92}, 
                "my_timestamp":                {"currency": false, "datatype": "TIMESTAMP", "precision": 26,         "scale": 3,  "signed": false, "size": 26,         "type": 93}, 
                "my_timestamp_with_time_zone": {"currency": false, "datatype": "TIMESTAMP", "precision": 32,         "scale": 3,  "signed": false, "size": 32,         "type": 93}}'''
        primary_key_specification_expected_json = json.dumps(json.loads(primary_key_specification_expected),
                                                             sort_keys=True)

        for record in wiretap.output_records:
            primary_key_specification_json = json.dumps(
                json.loads(record.header.values["jdbc.primaryKeySpecification"]), sort_keys=True)
            assert primary_key_specification_json == primary_key_specification_expected_json
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        try:
            database_connection.execute(f'drop table {table_name}')
        except:
            pass
        database_connection.close()


@database('postgresqlaurora')
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('wal2json_format, record_contents', [('TRANSACTION', 'OPERATION'),
                                                              ('CHUNKED_TRANSACTION', 'OPERATION'),
                                                              ('OPERATION', 'OPERATION')])
def test_aurora_postgres_cdc_client_primary_keys_headers(sdc_builder,
                                                         sdc_executor,
                                                         database,
                                                         wal2json_format,
                                                         record_contents):
    """
    Test to check all headers for primary keys are present in the output records.
    """

    try:

        database_connection = database.engine.connect()

        replication_slot_name = get_random_string(string.ascii_lowercase, 10)

        table_name = f'stf_{get_random_string(string.ascii_lowercase, 16)}'

        pipeline_builder = sdc_builder.get_pipeline_builder()
        postgres_cdc_client = pipeline_builder.add_stage('Aurora PostgreSQL CDC Client')
        postgres_cdc_client.set_attributes(batch_wait_time_in_ms=300000,
                                           max_batch_size_in_records=1,
                                           remove_replication_slot_on_close=True,
                                           poll_interval=1,
                                           replication_slot=replication_slot_name,
                                           ssl_mode='DISABLED',
                                           record_contents=record_contents,
                                           wal2json_format=wal2json_format,
                                           tables=[{'schema': 'public',
                                                    'table': table_name,
                                                    'excludePattern': ''}])
        wiretap = pipeline_builder.add_wiretap()
        postgres_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        database_transaction = database_connection.begin()
        logger.info('Creating source table %s in %s database ...', table_name, database.type)
        table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                 sqlalchemy.Column('TYPE', sqlalchemy.String(64), primary_key=True, quote=False),
                                 sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True, quote=False),
                                 sqlalchemy.Column('NAME', sqlalchemy.String(64), quote=False),
                                 sqlalchemy.Column('SURNAME', sqlalchemy.String(64), quote=False),
                                 sqlalchemy.Column('ADDRESS', sqlalchemy.String(64), quote=False),
                                 quote=False)
        table.create(database.engine)

        sdc_executor.start_pipeline(pipeline)

        database_transaction.commit()

        column_type = "'" + "Hobbit" + "'"
        column_id = 1
        column_name = "'" + "Bilbo" + "'"
        column_surname = "'" + "Baggins" + "'"

        column_address = "'" + "Bag End 0" + "'"
        database_transaction = database_connection.begin()
        sentence = f"insert into {table} " \
                   f"values ({column_type}, {column_id}, {column_name}, {column_surname}, {column_address})"
        sql = sqlalchemy.text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 1" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address}, TYPE = 'Fallohide' where TYPE = 'Hobbit' and ID = 1"
        sql = sqlalchemy.text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 2" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address}, ID = 2 where TYPE = 'Fallohide' and ID = 1"
        sql = sqlalchemy.text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 3" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address}, TYPE = 'Hobbit - Fallohide', ID = 3 where TYPE = 'Fallohide' and ID = 2"
        sql = sqlalchemy.text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 4" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address}, TYPE = 'Hobbit, Fallohide' where TYPE = 'Hobbit - Fallohide'"
        sql = sqlalchemy.text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 5" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address}, ID = 4 where ID = 3"
        sql = sqlalchemy.text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 6" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address} where ID = 4"
        sql = sqlalchemy.text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f"delete from {table_name}"
        sql = sqlalchemy.text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 8, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)
        records = wiretap.output_records
        assert len(records) == 8

        for record in records:
            if record.header.values["postgres.cdc.operation"] == 'U' or \
                    record.header.values["postgres.cdc.operation"] == 'UPDATE':

                assert "jdbc.primaryKey.before.type" in record.header.values
                assert "jdbc.primaryKey.before.id" in record.header.values
                assert "jdbc.primaryKey.after.type" in record.header.values
                assert "jdbc.primaryKey.after.id" in record.header.values

                assert record.header.values["jdbc.primaryKey.before.type"] is not None
                assert record.header.values["jdbc.primaryKey.before.id"] is not None
                assert record.header.values["jdbc.primaryKey.after.type"] is not None
                assert record.header.values["jdbc.primaryKey.after.id"] is not None

                column_address = record.field['address'].value

                if column_address == 'Bag End 1':
                    assert record.header.values["jdbc.primaryKey.before.type"] == "Hobbit"
                    assert record.header.values["jdbc.primaryKey.before.id"] == "1"
                    assert record.header.values["jdbc.primaryKey.after.type"] == "Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.id"] == "1"
                elif column_address == 'Bag End 2':
                    assert record.header.values["jdbc.primaryKey.before.type"] == "Fallohide"
                    assert record.header.values["jdbc.primaryKey.before.id"] == "1"
                    assert record.header.values["jdbc.primaryKey.after.type"] == "Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.id"] == "2"
                elif column_address == 'Bag End 3':
                    assert record.header.values["jdbc.primaryKey.before.type"] == "Fallohide"
                    assert record.header.values["jdbc.primaryKey.before.id"] == "2"
                    assert record.header.values["jdbc.primaryKey.after.type"] == "Hobbit - Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.id"] == "3"
                elif column_address == 'Bag End 4':
                    assert record.header.values["jdbc.primaryKey.before.type"] == "Hobbit - Fallohide"
                    assert record.header.values["jdbc.primaryKey.before.id"] == "3"
                    assert record.header.values["jdbc.primaryKey.after.type"] == "Hobbit, Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.id"] == "3"
                elif column_address == 'Bag End 5':
                    assert record.header.values["jdbc.primaryKey.before.type"] == "Hobbit, Fallohide"
                    assert record.header.values["jdbc.primaryKey.before.id"] == "3"
                    assert record.header.values["jdbc.primaryKey.after.type"] == "Hobbit, Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.id"] == "4"
                elif column_address == 'Bag End 6':
                    assert record.header.values["jdbc.primaryKey.before.type"] == "Hobbit, Fallohide"
                    assert record.header.values["jdbc.primaryKey.before.id"] == "4"
                    assert record.header.values["jdbc.primaryKey.after.type"] == "Hobbit, Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.id"] == "4"

            else:

                assert "jdbc.primaryKey.before.type" not in record.header.values
                assert "jdbc.primaryKey.before.id" not in record.header.values
                assert "jdbc.primaryKey.after.type" not in record.header.values
                assert "jdbc.primaryKey.after.id" not in record.header.values

            logger.info(f".....................")
            if record.header.values["postgres.cdc.operation"] == 'U' or \
                    record.header.values["postgres.cdc.operation"] == 'UPDATE':
                logger.info(f"column - address.................: {record.field['address'].value}")
                logger.info(f"jdbc.primaryKey.before.type: {record.header.values['jdbc.primaryKey.before.type']}")
                logger.info(f"jdbc.primaryKey.before.id..: {record.header.values['jdbc.primaryKey.before.id']}")
                logger.info(f"jdbc.primaryKey.after.type.: {record.header.values['jdbc.primaryKey.after.type']}")
                logger.info(f"jdbc.primaryKey.after.id...: {record.header.values['jdbc.primaryKey.after.id']}")
                logger.info(f"----------------------------------")

    finally:

        if pipeline is not None:
            if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
                sdc_executor.stop_pipeline(pipeline=pipeline, force=True)

        try:
            database.deactivate_and_drop_replication_slot(replication_slot_name)
        except:
            pass

        try:
            if table is not None:
                database_connection.execute(f'drop table {table_name}')
        except:
            pass

        try:
            database_connection.close()
        except:
            pass
