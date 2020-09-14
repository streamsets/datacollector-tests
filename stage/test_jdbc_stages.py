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

import copy
import datetime
import json
import logging
import math
import os
import random
import string
import tempfile
import time
from collections import OrderedDict

import pytest
import sqlalchemy
from streamsets.sdk.utils import Version
from streamsets.testframework.environments.databases import OracleDatabase, SQLServerDatabase, PostgreSqlDatabase
from streamsets.testframework.markers import credentialstore, database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Dima'},
    {'id': 2, 'name': 'Jarcec'},
    {'id': 3, 'name': 'Arvind'}
]
ROWS_TO_UPDATE = [
    {'id': 2, 'name': 'Eddie'},
    {'id': 4, 'name': 'Jarcec'}
]
LOOKUP_RAW_DATA = ['id'] + [str(row['id']) for row in ROWS_IN_DATABASE]
RAW_DATA = ['name'] + [row['name'] for row in ROWS_IN_DATABASE]

DEFAULT_DB2_SCHEMA = 'DB2INST1'


def _get_random_name(database, prefix='', length=5):
    """Generate a random string to use as a database object name.

    It handles letter case according to the database type, forcing upper-case (e.g. Oracle) or lower-case
    (e.g. Postgres).

    Args:
        database: a :obj:`streamsets.testframework.environment.Database` object.
        prefix: (:obj:`str`) add a prefix to the generated name. Default: ''.
        length: (:obj:`int`) number of characters of the generated name (without counting ``prefix``).

    """
    if isinstance(database, OracleDatabase):
        name = '{}{}'.format(prefix.upper(), get_random_string(string.ascii_uppercase))
    else:
        name = '{}{}'.format(prefix.lower(), get_random_string(string.ascii_lowercase))

    return name


def _get_random_schema_name(database, prefix='', length=5):
    """Generate a random string to use as a database schema name.

    It handles letter case according to the database type, forcing upper-case (e.g. Oracle) or lower-case
    (e.g. Postgres).

    For Oracle databases 'C##' is prepended to the final name. This is a workaround needed to work against
    Oracle multitenant databases (e.g. our Oracle 19c environment), since common users (schemas) must be start
    with that prefix.

    Args:
        database: a :obj:`streamsets.testframework.environment.Database` object.
        prefix: (:obj:`str`) add a prefix to the generated name. Default: ''.
        length: (:obj:`int`) number of characters of the generated name (without counting ``prefix``).

    """
    prefix = f'C##{prefix}' if isinstance(database, OracleDatabase) else prefix
    return _get_random_name(database, prefix=prefix, length=length)


def _create_table(table_name, database, schema_name=None):
    """Helper function to create a table with two columns: id (int, PK) and name (str).

    Args:
        table_name: (:obj:`str`) the name for the new table.
        database: a :obj:`streamsets.testframework.environment.Database` object.
        schema_name: (:obj:`str`, optional) when provided, create the new table in a specific schema; otherwise,
            the default schema for the engine’s database connection is used.

    Return:
        The new table as a sqlalchemy.Table object.

    """
    metadata = sqlalchemy.MetaData()

    if type(database) == SQLServerDatabase:
        table = sqlalchemy.Table(table_name,
                                 metadata,
                                 sqlalchemy.Column('name', sqlalchemy.String(32)),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True,
                                                   autoincrement=False),
                                 schema=schema_name)
    else:
        table = sqlalchemy.Table(table_name,
                                 metadata,
                                 sqlalchemy.Column('name', sqlalchemy.String(32)),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                                 schema=schema_name)

    logger.info('Creating table %s in %s database ...', table_name, database.type)
    table.create(database.engine)
    return table


def _create_schema(schema_name, database):
    """Create a new schema in the database.

    For RDBMs with no distinction between schema and database (e.g. MySQL), it creates a new database. For Oracle, it
    creates a new user. For databases with schema objects, it creates a new schema.

    Use ``_drop_schema()`` to remove schemas created by this function, to handle properly each case.

    Args:
        schema_name: (:obj:`str`) the schema name.
        database: a :obj:`streamsets.testframework.environment.Database` object.

    """
    if isinstance(database, OracleDatabase):
        database.engine.execute('CREATE USER {user} IDENTIFIED BY {pwd}'.format(user=schema_name, pwd=schema_name))
        database.engine.execute('ALTER USER {user} QUOTA UNLIMITED ON USERS'.format(user=schema_name))
        database.engine.execute('GRANT CONNECT, RESOURCE TO {user}'.format(user=schema_name))
    else:
        schema = sqlalchemy.schema.CreateSchema(schema_name)
        database.engine.execute(schema)


def _drop_schema(schema_name, database):
    """Remove a schema from the given database.

    Args:
        schema_name: (:obj:`str`) name of the schema to remove.
        database: a :obj:`streamsets.testframework.environment.Database` object.

    """
    if isinstance(database, OracleDatabase):
        database.engine.execute('DROP USER {user} CASCADE'.format(user=schema_name))
    else:
        sqlalchemy.schema.DropSchema(schema_name)


@credentialstore
@database
def test_jdbc_lookup_processor(sdc_builder, sdc_executor, database):
    """Simple JDBC Lookup processor test.
    Pipeline will enrich records with the 'name' by adding a field as 'FirstName'.
    The pipeline looks like:
        dev_raw_data_source >> jdbc_lookup >> trash
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)
    logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
    connection = database.engine.connect()
    connection.execute(table.insert(), ROWS_IN_DATABASE)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(LOOKUP_RAW_DATA))

    jdbc_lookup = pipeline_builder.add_stage('JDBC Lookup')
    query_str = f"SELECT name FROM {table_name} WHERE id = '${{record:value('/id')}}'"
    column_mappings = [dict(dataType='USE_COLUMN_TYPE',
                            columnName='name',
                            field='/FirstName')]
    jdbc_lookup.set_attributes(sql_query=query_str,
                               column_mappings=column_mappings)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> jdbc_lookup >> trash
    pipeline = pipeline_builder.build(title='JDBC Lookup').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    LOOKUP_EXPECTED_DATA = copy.deepcopy(ROWS_IN_DATABASE)
    for record in LOOKUP_EXPECTED_DATA:
        record.pop('id')
        record['FirstName'] = record.pop('name')

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline,
                                                 start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)
        rows_from_snapshot = [{list(record.field.keys())[1]: list(record.field.values())[1].value}
                              for record in snapshot[jdbc_lookup].output]
        assert rows_from_snapshot == LOOKUP_EXPECTED_DATA
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_tee_processor(sdc_builder, sdc_executor, database):
    """Simple JDBC Tee processor test.
    Pipeline will insert records into database and then pass generated database column 'id' to fields.
    The pipeline looks like:
        dev_raw_data_source >> jdbc_tee >> trash
    """
    if isinstance(database, OracleDatabase):
        pytest.skip('JDBC Tee Processor does not support Oracle')
    elif type(database) == SQLServerDatabase:
        pytest.skip('JDBC Tee Processor does not support SQL Server')

    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(RAW_DATA))

    jdbc_tee = pipeline_builder.add_stage('JDBC Tee')
    # Note that here ids are not inserted. Database generates them automatically.
    field_to_column_mapping = [dict(columnName='name',
                                    dataType='USE_COLUMN_TYPE',
                                    field='/name',
                                    paramValue='?')]
    generated_column_mappings = [dict(columnName='id',
                                      dataType='USE_COLUMN_TYPE',
                                      field='/id')]
    jdbc_tee.set_attributes(default_operation='INSERT',
                            field_to_column_mapping=field_to_column_mapping,
                            generated_column_mappings=generated_column_mappings,
                            table_name=table_name)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> jdbc_tee >> trash
    pipeline = pipeline_builder.build(title='JDBC Tee').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline,
                                                 start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # Verify the JDBC Tee processor has got new ids which were generated by database.
        rows_from_snapshot = [{list(item.field.keys())[0]: list(item.field.values())[0].value,
                               list(item.field.keys())[1]: int(list(item.field.values())[1].value)}
                              for item in snapshot[jdbc_tee].output]
        assert rows_from_snapshot == ROWS_IN_DATABASE
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
@pytest.mark.parametrize('use_multi_row', [True, False])
@sdc_min_version('3.0.0.0')  # stop_after_first_batch
def test_jdbc_tee_processor_multi_ops(sdc_builder, sdc_executor, database, use_multi_row):
    """JDBC Tee processor with multiple operations
    Pipeline will delete/update/insert records into database with one batch and then update 'id'
    field if it is inserted. The 'operation' field is used for the record header sdc.operation.type
    which defines the CRUD operation (1: Insert, 2: Delete, 3: Update). The pipeline looks like:
        dev_raw_data_source >> expression evaluator >> jdbc_tee >> trash
    """
    if isinstance(database, OracleDatabase):
        pytest.skip('JDBC Tee Processor does not support Oracle')
    elif type(database) == SQLServerDatabase:
        pytest.skip('JDBC Tee Processor does not support SQL Server')

    table_name = get_random_string(string.ascii_lowercase, 20)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    DATA = [
        {'operation': 2, 'name': 'Jarcec', 'id': 2},  # delete
        {'operation': 3, 'name': 'Hari', 'id': 3},  # update
        {'operation': 1, 'name': 'Eddie'}  # insert, id will be added by JDBC Tee
    ]
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='\n'.join(json.dumps(rec) for rec in DATA),
                                       stop_after_first_batch=True)

    HEADER_EXPRESSIONS = [dict(attributeToSet='sdc.operation.type',
                               headerAttributeExpression="${record:value('/operation')}")]
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = HEADER_EXPRESSIONS

    FIELD_TO_COLUMN = [dict(columnName='name', field='/name', paramValue='?')]
    jdbc_tee = pipeline_builder.add_stage('JDBC Tee')
    jdbc_tee.set_attributes(default_operation='INSERT',
                            field_to_column_mapping=FIELD_TO_COLUMN,
                            generated_column_mappings=[dict(columnName='id', field='/id')],
                            table_name=table_name,
                            use_multi_row_operation=use_multi_row)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> expression_evaluator >> jdbc_tee >> trash
    pipeline_title = 'JDBC Tee MultiOps MultiRow' if use_multi_row else 'JDBC Tee MultiOps SingleRow'
    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = _create_table(table_name, database)
    try:
        logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
        connection = database.engine.connect()
        # Passing only names to get the correct sequence numbers esp. PostgreSQL
        if type(database) == SQLServerDatabase:
            connection.execute(table.insert(), [{'id': row['id'], 'name': row['name']} for row in ROWS_IN_DATABASE])
        else:
            connection.execute(table.insert(), [{'name': row['name']} for row in ROWS_IN_DATABASE])

        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline,
                                                 start_pipeline=True).snapshot
        sequence_id = len(ROWS_IN_DATABASE)
        # Verify the database is updated.
        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        expected_data = [(row['name'], row['id']) for row in ROWS_IN_DATABASE]
        for record in DATA:
            if record['operation'] == 1:  # insert
                sequence_id += 1
                expected_data.append((record['name'], sequence_id))
            elif record['operation'] == 2:  # delete
                expected_data = [row for row in expected_data if row[1] != record['id']]
            elif record['operation'] == 3:  # update
                expected_data = [row if row[1] != record['id'] else (record['name'], row[1]) for row in expected_data]
        assert data_from_database == expected_data

        # Verify the JDBC Tee processor has the new ID which were generated by database.
        jdbc_tee_output = snapshot[jdbc_tee].output
        name_id_from_output = [(record.field['name'], record.field['id']) for record in jdbc_tee_output]
        assert name_id_from_output == [('Jarcec', 2), ('Hari', 3), ('Eddie', sequence_id)]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_query_executor(sdc_builder, sdc_executor, database):
    """Simple JDBC Query Executor test.
    Pipeline will insert records into database and then using sqlalchemy, the verification will happen
    that correct data is inserted into database.

    This is achieved by using a deduplicator which assures us that there is only one ingest to database.
    The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> jdbc_query_executor
                               record_deduplicator >> trash
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)

    DATA = ['id,name'] + [','.join(str(item) for item in rec.values()) for rec in ROWS_IN_DATABASE]
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(DATA))

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')

    jdbc_query_executor = pipeline_builder.add_stage('JDBC Query', type='executor')
    query_str = f"INSERT INTO {table_name} (name, id) VALUES ('${{record:value('/name')}}', '${{record:value('/id')}}')"

    if Version(sdc_builder.version) < Version('3.14.0'):
        jdbc_query_executor.set_attributes(sql_query=query_str)
    else:
        jdbc_query_executor.set_attributes(sql_queries=[query_str])

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> record_deduplicator >> jdbc_query_executor
    record_deduplicator >> trash
    pipeline = pipeline_builder.build(title='JDBC Query Executor').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(RAW_DATA) - 1)
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
@sdc_min_version('3.14.0')  # multiple queries execution
def test_jdbc_query_executor_multiple_queries(sdc_builder, sdc_executor, database):
    """Simple JDBC Query Executor test.
    Pipeline will insert records into database and then using sqlalchemy, the verification will happen
    that correct data is inserted into database.

    This is achieved by using a deduplicator which assures us that there is only one ingest to database.
    The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> jdbc_query_executor
                               record_deduplicator >> trash
    """
    table_name = f'stf_{get_random_string(string.ascii_lowercase, 20)}'
    table = _create_table(table_name, database)

    ROWS_IN_DATABASE_UPDATED = [
        {'id': 1, 'name': 'Alex'},
        {'id': 2, 'name': 'Alex'},
        {'id': 3, 'name': 'Alex'}
    ]

    DATA = ['id,name'] + [','.join(str(item) for item in rec.values()) for rec in ROWS_IN_DATABASE]
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(DATA))

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')

    jdbc_query_executor = pipeline_builder.add_stage('JDBC Query', type='executor')
    query_str1 = f"INSERT INTO {table_name} (name, id) VALUES ('${{record:value('/name')}}', '${{record:value('/id')}}')"
    query_str2 = f"UPDATE {table_name} SET name = 'Alex' WHERE name = '${{record:value('/name')}}'"

    jdbc_query_executor.set_attributes(sql_queries=[query_str1, query_str2])

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> record_deduplicator >> jdbc_query_executor
    record_deduplicator >> trash
    pipeline = pipeline_builder.build(title='JDBC Query Executor').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(RAW_DATA) - 1)
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE_UPDATED]
    finally:
        logger.info(f'Dropping table {table_name} in {database.type} database ...')
        table.drop(database.engine)


@database
@sdc_min_version('3.11.0')
def test_jdbc_query_executor_successful_query_event(sdc_builder, sdc_executor, database):
    """Simple JDBC Query Executor test for successful-query event type.
    Pipeline will insert records into database and then using sqlalchemy, the verification will happen
    that correct data is inserted into database. Event records are verified for successful-query event type.

    This is achieved by using a deduplicator which assures us that there is only one ingest to database.
    The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> jdbc_query_executor >= trash1
                               record_deduplicator >> trash2
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)

    DATA = ['id,name'] + [','.join(str(item) for item in rec.values()) for rec in ROWS_IN_DATABASE]
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(DATA))

    query_str = f"INSERT INTO {table_name} (name, id) VALUES ('${{record:value('/name')}}', '${{record:value('/id')}}')"

    jdbc_query_executor = pipeline_builder.add_stage('JDBC Query', type='executor')

    if Version(sdc_builder.version) < Version('3.14.0'):
        jdbc_query_executor.set_attributes(sql_query=query_str)
    else:
        jdbc_query_executor.set_attributes(sql_queries=[query_str])

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    trash1 = pipeline_builder.add_stage('Trash')
    trash2 = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> jdbc_query_executor >= trash1
    record_deduplicator >> trash2
    pipeline = pipeline_builder.build(title='JDBC Query Executor').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        event_records = snapshot[jdbc_query_executor.instance_name].event_records
        assert len(event_records) == 3
        assert 'successful-query' == event_records[0].header['values']['sdc.event.type']
        assert 'successful-query' == event_records[1].header['values']['sdc.event.type']
        assert 'successful-query' == event_records[2].header['values']['sdc.event.type']

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
@sdc_min_version('3.11.0')
def test_jdbc_query_executor_insert_query_result_count(sdc_builder, sdc_executor, database):
    """Simple JDBC Query Executor test for successful-query event type and query result count enabled.
    Pipeline will insert records into database and then using sqlalchemy, the verification will happen
    that correct data is inserted into database. Event records are verified for successful-query event type
    and query-result field for the insert query.

    This is achieved by using a deduplicator which assures us that there is only one ingest to database.
    The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> jdbc_query_executor >= trash1
                               record_deduplicator >> trash2
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)

    DATA = ['id,name'] + [','.join(str(item) for item in rec.values()) for rec in ROWS_IN_DATABASE]
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(DATA))

    query_str = f"INSERT INTO {table_name} (name, id) VALUES ('${{record:value('/name')}}', '${{record:value('/id')}}')"

    jdbc_query_executor = pipeline_builder.add_stage('JDBC Query', type='executor')

    jdbc_query_executor.set_attributes(include_query_result_count_in_events=True)

    if Version(sdc_builder.version) < Version('3.14.0'):
        jdbc_query_executor.set_attributes(sql_query=query_str)
    else:
        jdbc_query_executor.set_attributes(sql_queries=[query_str])

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    trash1 = pipeline_builder.add_stage('Trash')
    trash2 = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> jdbc_query_executor >= trash1
    record_deduplicator >> trash2
    pipeline = pipeline_builder.build(title='JDBC Query Executor').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        event_records = snapshot[jdbc_query_executor.instance_name].event_records
        assert len(event_records) == 3
        assert 'successful-query' == event_records[0].header['values']['sdc.event.type']
        assert 'successful-query' == event_records[1].header['values']['sdc.event.type']
        assert 'successful-query' == event_records[2].header['values']['sdc.event.type']

        assert '1 row(s) affected' == event_records[0].value['value']['query-result']['value']
        assert '1 row(s) affected' == event_records[1].value['value']['query-result']['value']
        assert '1 row(s) affected' == event_records[2].value['value']['query-result']['value']

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
@sdc_min_version('3.0.0.0')
def test_jdbc_query_executor_lifecycle_events(sdc_builder, sdc_executor, database):
    """Verify that the JDBC Query Executor will work properly when used inside pipeline lifecycle stages."""
    if isinstance(database, OracleDatabase):
        pytest.skip('This test does not support Oracle')
    elif type(database) == SQLServerDatabase:
        pytest.skip('This test does not support SQL Server')

    table_name = get_random_string(string.ascii_lowercase, 20)
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table_name,
                             metadata,
                             sqlalchemy.Column('user', sqlalchemy.String(50)),
                             sqlalchemy.Column('event', sqlalchemy.String(50)))
    logger.info('Creating table %s in %s database ...', table_name, database.type)
    table.create(database.engine)

    query = f"INSERT INTO {table_name} VALUES ('${{record:value('/user')}}', '${{record:attribute('sdc.event.type')}}')"

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'TEXT'
    source.raw_data='SOMETHING'

    trash = builder.add_stage('Trash')

    start_stage = builder.add_start_event_stage('JDBC Query')
    if Version(sdc_builder.version) < Version('3.14.0'):
        start_stage.set_attributes(sql_query=query)
    else:
        start_stage.set_attributes(sql_queries=[query])

    stop_stage = builder.add_stop_event_stage('JDBC Query')
    if Version(sdc_builder.version) < Version('3.14.0'):
        stop_stage.set_attributes(sql_query=query)
    else:
        stop_stage.set_attributes(sql_queries=[query])

    source >> trash

    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        result = database.engine.execute(table.select())
        db = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()

        assert db[0][0] == 'admin'
        assert db[0][1] == 'pipeline-start'
        assert db[1][0] == ''
        assert db[1][1] == 'pipeline-stop'
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_query_executor_failure_state(sdc_builder, sdc_executor, database):
    """Verify that the executor is properly called with the proper state on pipeline initialization failure."""
    table_name = get_random_string(string.ascii_lowercase, 20)
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table_name,
                             metadata,
                             sqlalchemy.Column('reason', sqlalchemy.String(50)))
    logger.info('Creating table %s in %s database ...', table_name, database.type)
    table.create(database.engine)

    query = f"INSERT INTO {table_name} VALUES ('${{record:value('/reason')}}')"

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('JDBC Multitable Consumer')
    source.table_configs=[{"tablePattern": 'this_table_do_not_exists'}]

    trash = builder.add_stage('Trash')

    stop_stage = builder.add_stop_event_stage('JDBC Query')
    if Version(sdc_builder.version) < Version('3.14.0'):
        stop_stage.set_attributes(sql_query=query)
    else:
        stop_stage.set_attributes(sql_queries=[query])

    source >> trash

    pipeline = builder.build().configure_for_environment(database)
    # Injecting failure - this URL won't exists, pipeline won't be able to start properly
    source.jdbc_connection_string = "jdbc:mysql://this-do-not-exists:3306/awesome-db"
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline, wait=False).wait_for_status('START_ERROR', ignore_errors=True)

        result = database.engine.execute(table.select())
        db = result.fetchall()
        result.close()

        assert db[0][0] == 'FAILURE'
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
@sdc_min_version('3.11.0')
def test_jdbc_query_executor_select_query_result_count(sdc_builder, sdc_executor, database):
    """Simple JDBC Query Executor test for successful-query event type and query result count enabled.
    Pipeline will insert records into database and then using sqlalchemy, the verification will happen
    that correct data is inserted into database and then the same data is queried. Event records are
    verified for successful-query event type and query-result field for the select query.

    This is achieved by using a deduplicator which assures us that there is only one ingest to database.
    The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> jdbc_query_executor1 >= jdbc_query_executor2 >= trash1
                               record_deduplicator >> trash2
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)

    DATA = ['id,name'] + [','.join(str(item) for item in rec.values()) for rec in ROWS_IN_DATABASE]
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(DATA))

    query_str1 = f"INSERT INTO {table_name} (name, id) VALUES ('${{record:value('/name')}}', '${{record:value('/id')}}')"
    query_str2 = f"SELECT * FROM {table_name}"

    jdbc_query_executor1 = pipeline_builder.add_stage('JDBC Query', type='executor')
    if Version(sdc_builder.version) < Version('3.14.0'):
        jdbc_query_executor1.set_attributes(sql_query=query_str1)
    else:
        jdbc_query_executor1.set_attributes(sql_queries=[query_str1])

    jdbc_query_executor2 = pipeline_builder.add_stage('JDBC Query', type='executor')

    jdbc_query_executor2.set_attributes(include_query_result_count_in_events=True)

    if Version(sdc_builder.version) < Version('3.14.0'):
        jdbc_query_executor2.set_attributes(sql_query=query_str2)
    else:
        jdbc_query_executor2.set_attributes(sql_queries=[query_str2])

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    trash1 = pipeline_builder.add_stage('Trash')
    trash2 = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> jdbc_query_executor1 >= jdbc_query_executor2 >= trash1
    record_deduplicator >> trash2
    pipeline = pipeline_builder.build(title='JDBC Query Executor').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        event_records = snapshot[jdbc_query_executor2.instance_name].event_records
        assert len(event_records) == 3
        assert 'successful-query' == event_records[0].header['values']['sdc.event.type']
        assert 'successful-query' == event_records[1].header['values']['sdc.event.type']
        assert 'successful-query' == event_records[2].header['values']['sdc.event.type']

        assert '3 row(s) returned' == event_records[0].value['value']['query-result']['value']
        assert '3 row(s) returned' == event_records[1].value['value']['query-result']['value']
        assert '3 row(s) returned' == event_records[2].value['value']['query-result']['value']

        result = database.engine.execute(table.select())
        result.close()
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
@sdc_min_version('3.11.0')
def test_jdbc_query_executor_failed_query_event(sdc_builder, sdc_executor, database):
    """Simple JDBC Query Executor test for failed-query event type.
    Pipeline will try to insert records into a non-existing table and the query would fail.
    Event records are verified for failed-query event type.

    This is achieved by using a deduplicator which assures us that there is only one ingest to database.
    The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> jdbc_query_executor >= trash1
                               record_deduplicator >> trash2
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)

    DATA = ['id,name'] + [','.join(str(item) for item in rec.values()) for rec in ROWS_IN_DATABASE]
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(DATA))
    invalid_table = "INVALID_TABLE"
    query_str = f"INSERT INTO {invalid_table} (name, id) VALUES ('${{record:value('/name')}}', '${{record:value('/id')}}')"

    jdbc_query_executor = pipeline_builder.add_stage('JDBC Query', type='executor')

    if Version(sdc_builder.version) < Version('3.14.0'):
        jdbc_query_executor.set_attributes(sql_query=query_str)
    else:
        jdbc_query_executor.set_attributes(sql_queries=[query_str])

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    trash1 = pipeline_builder.add_stage('Trash')
    trash2 = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> jdbc_query_executor >= trash1
    record_deduplicator >> trash2
    pipeline = pipeline_builder.build(title='JDBC Query Executor').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        event_records = snapshot[jdbc_query_executor.instance_name].event_records
        assert len(event_records) == 3
        assert 'failed-query' == event_records[0].header['values']['sdc.event.type']
        assert 'failed-query' == event_records[1].header['values']['sdc.event.type']
        assert 'failed-query' == event_records[2].header['values']['sdc.event.type']

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == []
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
@sdc_min_version('3.10.0')
@pytest.mark.parametrize('enable_parallel_execution', [True, False])
def test_jdbc_query_executor_parallel_query_execution(sdc_builder, sdc_executor, database, enable_parallel_execution):
    """Test JDBC Query Executor's parallel query execution mode.

    Pipeline will insert records into database, then update the records.
    Using sqlalchemy, we verify that correct data was inserted (and updated) in the database.

    Pipeline configuration:
        dev_raw_data_source >> jdbc_query_executor
    """


    table_name = get_random_string(string.ascii_uppercase, 20)
    table = _create_table(table_name, database)

    # Make sure that we properly escape the table name. Ideally we would do escape for all databases, but since we
    # know that all except postgre are passing, we only escape for Postgre for now.
    enclosed_table = f'"{table_name}"' if type(database) == PostgreSqlDatabase else table_name

    # first, the inserts - they will run in parallel,
    # then all the updates will run sequentially
    # net result is all records should get updated to the (last) new value.
    # otherwise we've failed.
    statements = []
    for rec in ROWS_IN_DATABASE:
        statements.extend([f"INSERT INTO {enclosed_table} (name, id) VALUES ('{rec['name']}', {rec['id']})",
                           f"UPDATE {enclosed_table} SET name = 'bob' WHERE id = {rec['id']}",
                           f"UPDATE {enclosed_table} SET name = 'MERRICK' WHERE id = {rec['id']}"])
    # convert to string - Dev Raw Data Source Data Format tab does not seem
    # to "unroll" the array into newline-terminated records.
    statements = "\n".join(statements)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=statements)

    jdbc_query_executor = pipeline_builder.add_stage('JDBC Query', type='executor')

    query_str = "${record:value('/text')}"

    jdbc_query_executor.set_attributes(enable_parallel_queries=enable_parallel_execution,
                                       maximum_pool_size=2,
                                       minimum_idle_connections=2)

    if Version(sdc_builder.version) < Version('3.14.0'):
        jdbc_query_executor.set_attributes(sql_query=query_str)
    else:
        jdbc_query_executor.set_attributes(sql_queries=[query_str])

    dev_raw_data_source >> jdbc_query_executor

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_IN_DATABASE)*3)
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == [('MERRICK', record['id']) for record in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


def _create_jdbc_producer_pipeline(pipeline_builder, pipeline_title, raw_data, table_name, operation):
    """Helper function to create and return a pipeline with JDBC Producer
    The Deduplicator assures there is only one ingest to database. The pipeline looks like:
    The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> jdbc_producer
                               record_deduplicator >> trash
    """
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')

    FIELD_MAPPINGS = [dict(field='/id', columnName='id'),
                      dict(field='/name', columnName='name')]
    jdbc_producer = pipeline_builder.add_stage('JDBC Producer')
    jdbc_producer.set_attributes(default_operation=operation,
                                 table_name=table_name,
                                 field_to_column_mapping=FIELD_MAPPINGS,
                                 stage_on_record_error='STOP_PIPELINE')

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> record_deduplicator >> jdbc_producer
    record_deduplicator >> trash

    return pipeline_builder.build(title=pipeline_title)


@database
def test_jdbc_producer_insert(sdc_builder, sdc_executor, database):
    """Simple JDBC Producer test with INSERT operation.
    The pipeline inserts records into the database and verify that correct data is in the database.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)

    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    pipeline = _create_jdbc_producer_pipeline(pipeline_builder, 'JDBC Producer Insert', DATA, table_name, 'INSERT')
    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_IN_DATABASE))
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database('mysql', 'postgresql')
def test_jdbc_producer_insert_type_err(sdc_builder, sdc_executor, database):
    """This test covers invalid type coersion - writing string into int column. As different databases works differently,
    we can't assert this across all supported databases. MySQL and PostgreSQL behaves the same way and we can properly
    catch and generate JDBC_23. Other databases report coercion issues much later in the query cycle, sometimes even
    in a way where we can't understand what and why has happened.
    """

    ROWS_IN_DATABASE = [
        {'id': 1, 'name': 'Dima'},
        {'id': 'X', 'name': 'Jarcec'},
        {'id': 3, 'name': 'Arvind'}
    ]

    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)

    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    FIELD_MAPPINGS = [dict(field='/id', columnName='id', dataType='INTEGER'),
                      dict(field='/name', columnName='name', dataType='STRING')]
    jdbc_producer = pipeline_builder.add_stage('JDBC Producer')
    jdbc_producer.set_attributes(default_operation='INSERT',
                                 table_name=table_name,
                                 field_to_column_mapping=FIELD_MAPPINGS,
                                 stage_on_record_error='TO_ERROR')

    dev_raw_data_source >> jdbc_producer

    pipeline = pipeline_builder.build(title="JDBC producer with error")

    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.get_pipeline_status(pipeline).wait_for_status('FINISHED')

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()

        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE
                                      if record['id'] != 'X']

        stage = snapshot[jdbc_producer.instance_name]

        assert 'JDBC_23' == stage.error_records[0].header['errorCode']

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


# SDC-15039: Data loss when JDBC Producer doesn't match any columns
@database
@pytest.mark.parametrize('multi_row', [True, False])
@pytest.mark.parametrize('field_mapping', [True, False])
def test_jdbc_producer_no_implicit_mapping(sdc_builder, sdc_executor, database, multi_row, field_mapping):
    """This test covers situation when neither of the record fields matches the destination table - in such cases
    the record should ended up in error stream.
    """
    if isinstance(database, OracleDatabase) and not field_mapping:
        pytest.skip('In case we depend on native mapping, Oracle will fail due to the upper-casing logic.')

    # Every second record have columns not available in the target table and should end up inside error stream
    INSERT_DATA = [
        {'id': 1, 'name': 'Dima'},
        {'date_of_birth': 'yesterday'},
        {'id': 2, 'name': 'Arvind'},
        {'date_of_birth': 'tomorrow'},
    ]

    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)

    DATA = '\n'.join(json.dumps(rec) for rec in INSERT_DATA)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    # Let's try with implicit?
    FIELD_MAPPINGS = [dict(field='/id', columnName='id', dataType='INTEGER'),
                      dict(field='/name', columnName='name', dataType='STRING')] if field_mapping else []
    jdbc_producer = pipeline_builder.add_stage('JDBC Producer')
    jdbc_producer.set_attributes(default_operation='INSERT',
                                 table_name=table_name,
                                 use_multi_row_operation=multi_row,
                                 field_to_column_mapping=FIELD_MAPPINGS,
                                 stage_on_record_error='TO_ERROR')

    dev_raw_data_source >> jdbc_producer

    pipeline = pipeline_builder.build().configure_for_environment(database)

    sdc_executor.add_pipeline(pipeline)

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.get_pipeline_status(pipeline).wait_for_status('FINISHED')

        stage = snapshot[jdbc_producer.instance_name]
        assert len(stage.error_records) == 2
        assert 'JDBC_90' == stage.error_records[0].header['errorCode']
        assert 'JDBC_90' == stage.error_records[1].header['errorCode']

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()

        assert data_from_database == [(record['name'], record['id']) for record in INSERT_DATA if 'id' in record]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_producer_insert_multiple_types(sdc_builder, sdc_executor, database):
    """Simple JDBC Producer test with INSERT operation.
    The pipeline inserts 1000 records of multiple types.
    The pipeline should look like:
        dev_data_generator >> jdbc_producer
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.fields_to_generate = [
        {'field': 'field1', 'type': 'STRING'},
        {'field': 'field2', 'type': 'DATETIME'},
        {'field': 'field3', 'type': 'INTEGER'},
        {'field': 'field4', 'precision': 10, 'scale': 2, 'type': 'DECIMAL'},
        {'field': 'field5', 'type': 'DOUBLE'}
    ]
    batch_size = 10000
    dev_data_generator.set_attributes(delay_between_batches=0, batch_size=batch_size)

    table_name = get_random_string(string.ascii_lowercase, 20)
    metadata = sqlalchemy.MetaData()

    table = sqlalchemy.Table(table_name,
                             metadata,
                             sqlalchemy.Column('field1', sqlalchemy.String(50)),
                             sqlalchemy.Column('field2', sqlalchemy.DateTime),
                             sqlalchemy.Column('field3', sqlalchemy.Integer),
                             sqlalchemy.Column('field4', sqlalchemy.DECIMAL(10, 2)),
                             sqlalchemy.Column('field5', sqlalchemy.Float),
                             schema=None)

    logger.info('Creating table %s in %s database ...', table_name, database.type)
    table.create(database.engine)

    FIELD_MAPPINGS = [dict(field='/field1', columnName='field1', dataType='STRING'),
                      dict(field='/field2', columnName='field2', dataType='DATETIME'),
                      dict(field='/field3', columnName='field3', dataType='INTEGER'),
                      dict(field='/field4', columnName='field4', dataType='DECIMAL'),
                      dict(field='/field5', columnName='field5', dataType='FLOAT')]

    jdbc_producer = pipeline_builder.add_stage('JDBC Producer')
    jdbc_producer.set_attributes(default_operation='INSERT',
                                 table_name=table_name,
                                 field_to_column_mapping=FIELD_MAPPINGS,
                                 stage_on_record_error='TO_ERROR')

    dev_data_generator >> jdbc_producer

    pipeline = pipeline_builder.build(title="JDBC producer multiple types")

    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(batch_size, timeout_sec=3600)
        snapshot = sdc_executor.capture_snapshot(pipeline).snapshot
        sdc_executor.stop_pipeline(pipeline).wait_for_stopped()

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()

        assert len(data_from_database) > batch_size

        stage = snapshot[jdbc_producer.instance_name]

        assert len(stage.error_records) == 0

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


# SDC-10786: This test intends to cover the case really precise decimals being inserted into a Float column in MSSQL
@database('sqlserver')
def test_mssql_producer_bigdecimal(sdc_builder, sdc_executor, database):
    """
    Insert a Decimal value with up to 38 decimals into a Float column in MSSQL.
    This will look like:
    dev_data_generator >> jdbc_producer
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column('a_value', sqlalchemy.Float()),
        sqlalchemy.Column('b_value', sqlalchemy.Float()),
        sqlalchemy.Column('c_value', sqlalchemy.Float()),
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, autoincrement=False)
    )
    table.create(database.engine)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.fields_to_generate = [{'field': 'id', 'type': 'INTEGER'},
                                             {'field': 'a_value', 'precision': 50, 'scale': 40, 'type': 'DECIMAL'},
                                             {'field': 'b_value', 'precision': 5, 'scale': 2, 'type': 'DECIMAL'},
                                             {'field': 'c_value', 'type': 'DECIMAL'}]
    dev_data_generator.batch_size = 1

    FIELD_MAPPINGS = [dict(field='/id', columnName='id'),
                      dict(field='/a_value', columnName='a_value'),
                      dict(field='/b_value', columnName='b_value'),
                      dict(field='/c_value', columnName='c_value')]

    jdbc_producer = pipeline_builder.add_stage('JDBC Producer')
    jdbc_producer.set_attributes(default_operation='INSERT',
                                 table_name=table_name,
                                 field_to_column_mapping=FIELD_MAPPINGS,
                                 stage_on_record_error='STOP_PIPELINE')
    dev_data_generator >> jdbc_producer

    pipeline = pipeline_builder.build('MSSQL BigDecimal')
    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, wait=True).snapshot

        sdc_executor.stop_pipeline(pipeline)

        records = [record.field for record in snapshot[dev_data_generator.instance_name].output]

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])  # order by id
        result.close()

        assert len(data_from_database) == 1

        assert math.isclose(float(str(records[0]['a_value'])), data_from_database[0][0], rel_tol=0.02)
        assert math.isclose(float(str(records[0]['b_value'])), data_from_database[0][1], rel_tol=0.02)
        assert math.isclose(float(str(records[0]['c_value'])), data_from_database[0][2], rel_tol=0.02)
        assert math.isclose(float(str(records[0]['id'])), data_from_database[0][3], rel_tol=0.02)
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_producer_coerced_insert(sdc_builder, sdc_executor, database):
    """Extension of the Simple JDBC Producer test with INSERT operation.
    The pipeline inserts records into the database.
     In one record, data is represented as type String, where column is type Integer.
     This should be passed to the database to coerce.
     Verify that correct data is in the database.

     Please note the use of local COERCE_ROWS_IN_DATABASE to insert
     and global ROWS_IN_DATABASE to verify.

     COERCE_ has id (integer) set to string.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)

    COERCE_ROWS_IN_DATABASE = [
        {'id': '1', 'name': 'Dima'},
        {'id': '2', 'name': 'Jarcec'},
        {'id': '3', 'name': 'Arvind'}
    ]

    DATA = '\n'.join(json.dumps(rec) for rec in COERCE_ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    pipeline = _create_jdbc_producer_pipeline(pipeline_builder, 'JDBC Producer Insert', DATA, table_name, 'INSERT')
    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_IN_DATABASE))
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_producer_delete(sdc_builder, sdc_executor, database):
    """Simple JDBC Producer test with DELETE operation.
    The pipeline deletes records from the database and verify that correct data is in the database.
    Records are deleted if the primary key is matched irrespective of other column values.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)
    logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
    connection = database.engine.connect()
    connection.execute(table.insert(), ROWS_IN_DATABASE)

    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_TO_UPDATE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    pipeline = _create_jdbc_producer_pipeline(pipeline_builder, 'JDBC Producer Delete', DATA, table_name, 'DELETE')
    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_TO_UPDATE))
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table.select())
        data_from_database = result.fetchall()
        result.close()
        removed_ids = [record['id'] for record in ROWS_TO_UPDATE]
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE if
                                      record['id'] not in removed_ids]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_producer_update(sdc_builder, sdc_executor, database):
    """Simple JDBC Producer test with UPDATE operation.
    The pipeline updates records from the database and verify that correct data is in the database.
    Records with matching primary key are updated, and no action for unmatched records.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)
    logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
    connection = database.engine.connect()
    connection.execute(table.insert(), ROWS_IN_DATABASE)

    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_TO_UPDATE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    pipeline = _create_jdbc_producer_pipeline(pipeline_builder, 'JDBC Producer Update', DATA, table_name, 'UPDATE')
    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_TO_UPDATE))
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        updated_names = {record['id']: record['name'] for record in ROWS_IN_DATABASE}
        updated_names.update({record['id']: record['name'] for record in ROWS_TO_UPDATE})
        assert data_from_database == [(updated_names[record['id']], record['id']) for record in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


# SDC-10562: Row-level stage errors not being caught at pipeline
@sdc_min_version('3.0.0.0')
@database
def test_jdbc_producer_multirow_with_duplicates(sdc_builder, sdc_executor, database):
    """
    Make sure that when using Multi Row insert, data related errors are send to error stream.
    """
    if type(database) == SQLServerDatabase:
        pytest.skip('This test is trying to insert explicit value to identity column which is not supported on SQL Server')

    table_name = get_random_string(string.ascii_lowercase, 15)

    builder = sdc_builder.get_pipeline_builder()

    # Generate batch that will repeat the same primary key in the middle of the batch (on third row)
    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'JSON'
    source.raw_data = """{"id" : 1}\n{"id" : 2}\n{"id" : 1}\n{"id" : 3}"""

    producer = builder.add_stage('JDBC Producer')
    producer.table_name = table_name
    producer.field_to_column_mapping = []
    producer.default_operation = 'INSERT'
    producer.use_multi_row_operation = True
    if database.type == 'Oracle':
        producer.enclose_object_names = True

    source >> producer

    pipeline = builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True)
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Since we are inserting duplicate primary key, the batch should fail
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 4
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 4
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 0

        # And similarly the database side should be empty as well
        result = database.engine.execute(table.select())
        data_from_database = result.fetchall()
        result.close()
        assert len(data_from_database) == 0

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_producer_multitable(sdc_builder, sdc_executor, database):
    """Test for JDBC Producer with multiple destination table. We create 3 tables in the default schema and use an EL
    expression to insert records according to the /table record field.

    Pipeline:
        dev_raw_data_source >> record_deduplicator >> jdbc_producer
                               record_deduplicator >> trash

    """
    table1_name = _get_random_name(database, prefix='stf_table_')
    table2_name = _get_random_name(database, prefix='stf_table_')
    table3_name = _get_random_name(database, prefix='stf_table_')

    table1 = _create_table(table1_name, database)
    table2 = _create_table(table2_name, database)
    table3 = _create_table(table3_name, database)

    ROWS = [{'table': table1_name, 'id': 1, 'name': 'Roger Federer'},
            {'table': table2_name, 'id': 2, 'name': 'Rafael Nadal'},
            {'table': table3_name, 'id': 3, 'name': 'Dominic Thiem'}]

    INPUT_DATA = '\n'.join(json.dumps(rec) for rec in ROWS)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline = _create_jdbc_producer_pipeline(pipeline_builder, 'JDBC Producer Multitable Insert', INPUT_DATA,
                                              "${record:value('/table')}", 'INSERT')

    # JDBC Producer's "Table Name" property is converted to uppercase through the configure_for_environment() method
    # when database is Oracle. However EL function names are case-sensitive; we overwrite it afterwards to avoid an EL
    # error.
    pipeline.configure_for_environment(database)
    pipeline[2].set_attributes(table_name="${record:value('/table')}")

    # For Oracle, the default value of JDBC Producer's "Schema Name" property in the database environment is the
    # database name, but it should be the username instead.
    if isinstance(database, OracleDatabase):
        pipeline[2].set_attributes(schema_name=database.username.upper())

    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS))
        sdc_executor.stop_pipeline(pipeline)

        result1 = database.engine.execute(table1.select())
        result2 = database.engine.execute(table2.select())
        result3 = database.engine.execute(table3.select())

        data1 = result1.fetchall()
        data2 = result2.fetchall()
        data3 = result3.fetchall()

        assert data1 == [(ROWS[0]['name'], ROWS[0]['id'])]
        assert data2 == [(ROWS[1]['name'], ROWS[1]['id'])]
        assert data3 == [(ROWS[2]['name'], ROWS[2]['id'])]

        result1.close()
        result2.close()
        result3.close()

    finally:
        logger.info('Dropping tables %s, %s, %s in %s database...', table1_name, table2_name, table3_name,
                    database.type)
        table1.drop(database.engine)
        table2.drop(database.engine)
        table3.drop(database.engine)


# Test SDC-10719
@database
@sdc_min_version('3.8.0')
def test_jdbc_producer_multischema(sdc_builder, sdc_executor, database):
    """Test for JDBC Producer in a multischema scenario with a single destination table for each schema. We create 3
    schemas with one table for each, with the same name. Then we use an EL expression to insert records according to
    the /schema record field.

    Pipeline:
        dev_raw_data_source >> record_deduplicator >> jdbc_producer
                               record_deduplicator >> trash

    """
    schema1_name = _get_random_schema_name(database, prefix='stf_schema_')
    schema2_name = _get_random_schema_name(database, prefix='stf_schema_')
    schema3_name = _get_random_schema_name(database, prefix='stf_schema_')
    table_name = _get_random_name(database, prefix='stf_table_')

    _create_schema(schema1_name, database)
    _create_schema(schema2_name, database)
    _create_schema(schema3_name, database)

    table1 = _create_table(table_name, database, schema_name=schema1_name)
    table2 = _create_table(table_name, database, schema_name=schema2_name)
    table3 = _create_table(table_name, database, schema_name=schema3_name)

    ROWS = [{'schema': schema1_name, 'id': 1, 'name': 'Roger Federer'},
            {'schema': schema2_name, 'id': 2, 'name': 'Rafael Nadal'},
            {'schema': schema3_name, 'id': 3, 'name': 'Dominic Thiem'}]

    INPUT_DATA = '\n'.join(json.dumps(rec) for rec in ROWS)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline = _create_jdbc_producer_pipeline(pipeline_builder, 'JDBC Producer Multischema Insert', INPUT_DATA,
                                              table_name, 'INSERT')

    # JDBC Producer's "Schema Name" property is set through the `database` environment under some circumstances
    # (e.g. Sql Server database). We overwrite it afterwards for the test.
    pipeline.configure_for_environment(database)
    pipeline[2].set_attributes(schema_name="${record:value('/schema')}")

    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS))
        sdc_executor.stop_pipeline(pipeline)

        result1 = database.engine.execute(table1.select())
        result2 = database.engine.execute(table2.select())
        result3 = database.engine.execute(table3.select())

        data1 = result1.fetchall()
        data2 = result2.fetchall()
        data3 = result3.fetchall()

        assert data1 == [(ROWS[0]['name'], ROWS[0]['id'])]
        assert data2 == [(ROWS[1]['name'], ROWS[1]['id'])]
        assert data3 == [(ROWS[2]['name'], ROWS[2]['id'])]

        result1.close()
        result2.close()
        result3.close()

    finally:
        logger.info('Dropping table %s in schemas...', table_name)
        table1.drop(database.engine)
        table2.drop(database.engine)
        table3.drop(database.engine)

        logger.info('Dropping schemas %s, %s, %s...', schema1_name, schema2_name, schema3_name)
        _drop_schema(schema1_name, database)
        _drop_schema(schema2_name, database)
        _drop_schema(schema3_name, database)


# Test SDC-10719
@database
@sdc_min_version('3.8.0')
def test_jdbc_producer_multischema_multitable(sdc_builder, sdc_executor, database):
    """Test a JDBC Producer in a multischema scenario with different destination tables for each schema. We create 3
    schemas with one table for each, with different names. Then we use an EL expressions to insert records according to
    the /schema and /table record fields.

    There were a limitation in previous versions that affected to MySQL and MemSQL. These RDBMs do not differentiate
    between schema and database. SDC used the database configured in the JDBC connection string, and looked for database
    metadata filtering by database+schema. If the schema were other than the database of the connection string, metadata
    could not be retrieved. This was a problem in a multischema scenario, where several schemas are employed.

    Pipeline:
        dev_raw_data_source >> record_deduplicator >> jdbc_producer
                               record_deduplicator >> trash

    """
    schema1_name = _get_random_schema_name(database, prefix='stf_schema_')
    schema2_name = _get_random_schema_name(database, prefix='stf_schema_')
    schema3_name = _get_random_schema_name(database, prefix='stf_schema_')
    table1_name = _get_random_name(database, prefix='stf_table_')
    table2_name = _get_random_name(database, prefix='stf_table_')
    table3_name = _get_random_name(database, prefix='stf_table_')

    _create_schema(schema1_name, database)
    _create_schema(schema2_name, database)
    _create_schema(schema3_name, database)

    table1 = _create_table(table1_name, database, schema_name=schema1_name)
    table2 = _create_table(table2_name, database, schema_name=schema2_name)
    table3 = _create_table(table3_name, database, schema_name=schema3_name)

    ROWS = [{'schema': schema1_name, 'table': table1_name, 'id': 1, 'name': 'Roger Federer'},
            {'schema': schema2_name, 'table': table2_name, 'id': 2, 'name': 'Rafael Nadal'},
            {'schema': schema3_name, 'table': table3_name, 'id': 3, 'name': 'Dominic Thiem'}]

    INPUT_DATA = '\n'.join(json.dumps(rec) for rec in ROWS)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline = _create_jdbc_producer_pipeline(pipeline_builder, 'JDBC Producer Multischema and Multitable Insert',
                                              INPUT_DATA, "${record:value('/table')}", 'INSERT')

    # JDBC Producer's "Schema Name" property is set through the `database` environment under some circumstances
    # (e.g. Sql Server database). We overwrite it afterwards for the test.
    pipeline.configure_for_environment(database)
    pipeline[2].set_attributes(schema_name="${record:value('/schema')}")

    # JDBC Producer's "Table Name" property is converted to uppercase through the configure_for_environment() method
    # when database is Oracle. However EL function names are case-sensitive; we overwrite it afterwards to avoid an EL
    # error.
    pipeline[2].set_attributes(table_name="${record:value('/table')}")

    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS))
        sdc_executor.stop_pipeline(pipeline)

        result1 = database.engine.execute(table1.select())
        result2 = database.engine.execute(table2.select())
        result3 = database.engine.execute(table3.select())

        data1 = result1.fetchall()
        data2 = result2.fetchall()
        data3 = result3.fetchall()

        assert data1 == [(ROWS[0]['name'], ROWS[0]['id'])]
        assert data2 == [(ROWS[1]['name'], ROWS[1]['id'])]
        assert data3 == [(ROWS[2]['name'], ROWS[2]['id'])]

        result1.close()
        result2.close()
        result3.close()

    finally:
        logger.info('Dropping tables %s, %s, %s...', table1_name, table2_name, table3_name)
        table1.drop(database.engine)
        table2.drop(database.engine)
        table3.drop(database.engine)

        logger.info('Dropping schemas %s, %s, %s...', schema1_name, schema2_name, schema3_name)
        _drop_schema(schema1_name, database)
        _drop_schema(schema2_name, database)
        _drop_schema(schema3_name, database)


# SDC-11063: Do not reoder update statements in JDBC destination
@sdc_min_version('3.0.0.0')
@pytest.mark.parametrize('multi_row', [True, False])
@database
def test_jdbc_producer_ordering(sdc_builder, sdc_executor, multi_row, database):
    """Ensure that variously intertwined operations won't be executed out of order in harmful way."""
    table_name = get_random_string(string.ascii_lowercase, 20)
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True, autoincrement=False),
        sqlalchemy.Column('a', sqlalchemy.Integer, quote=True),
        sqlalchemy.Column('b', sqlalchemy.Integer, quote=True)
    )

    RAW_DATA = [
        # Update id=5
        {"op": 3, "id": 5, "a": 2, "b": 2},
        # Insert id=4
        {"op": 1, "id": 4, "a": 1, "b": 1},
        # Update id=4
        {"op": 3, "id": 4, "a": 2, "b": 2},
        # Delete id=5
        {"op": 2, "id": 5},

        # Insert id=1
        {"op": 1, "id": 1, "a": 1, "b": 1},
        # Update id=1
        {"op": 3, "id": 1, "a": 2},
        # Insert id=2
        {"op": 1, "id": 2, "a": 1, "b": 1},
        # Delete id=2
        {"op": 2, "id": 2},
        # Update id=1
        {"op": 3, "id": 1, "a": 2, "b": 2},
        # Insert id=3
        {"op": 1, "id": 3, "a": 1, "b": 1},
        # Update id=1
        {"op": 3, "id": 1, "a": 3},
        # Update id=3
        {"op": 3, "id": 3, "a": 5},
        # Delete id=3
        {"op": 2, "id": 3}
    ]

    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'JSON'
    source.raw_data = '\n'.join(json.dumps(rec) for rec in RAW_DATA)

    expression = builder.add_stage('Expression Evaluator')
    expression.header_attribute_expressions = [
        {'attributeToSet': 'sdc.operation.type', 'headerAttributeExpression': '${record:value("/op")}'}
    ]

    remover = builder.add_stage('Field Remover')
    remover.set_attributes(fields=['/op'], action='REMOVE')

    producer = builder.add_stage('JDBC Producer')
    producer.field_to_column_mapping = []
    producer.default_operation = 'UPDATE'
    producer.table_name = table_name
    producer.use_multi_row_operation = multi_row
    if database.type == 'Oracle':
        producer.enclose_object_names = True

    source >> expression >> remover >> producer

    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        # The table will start with single row (id=5)
        logger.info('Inserting rows into %s in %s database', table_name, database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), {'id': 5, 'a': 1, 'b': 1})

        # Finally run the pipeline and verify it's outcome
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        result = database.engine.execute(table.select())
        db = sorted(result.fetchall(), key=lambda row: row[0])  # order by id
        result.close()

        assert len(db) == 2

        # id=1
        assert 1 == db[0][0]
        assert 3 == db[0][1]
        assert 2 == db[0][2]

        # id=5
        assert 4 == db[1][0]
        assert 2 == db[1][1]
        assert 2 == db[1][2]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


# SDC-11092: Improve the ability of JDBC Destination to cover non-standard Data related SQL Error codes
@sdc_min_version('3.0.0.0')
@pytest.mark.parametrize('multi_row', [True, False])
@database('oracle')
def test_jdbc_producer_oracle_data_errors(sdc_builder, sdc_executor, multi_row, database):
    """Ensure that data related error in Oracle will be sent to eror stream rather then shutting the pipeline down."""
    table_name = get_random_string(string.ascii_lowercase, 20)
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('STR', sqlalchemy.String(2)),
    )

    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'JSON'
    source.raw_data = '{"ID" : 1, "STR": "Longer then 2 characters"}'

    producer = builder.add_stage('JDBC Producer')
    producer.field_to_column_mapping = []
    producer.default_operation = 'INSERT'
    producer.table_name = table_name
    producer.use_multi_row_operation = multi_row

    source >> producer

    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # The table in database needs to be empty
        result = database.engine.execute(table.select())
        db = sorted(result.fetchall(), key=lambda row: row[0])  # order by id
        result.close()
        assert len(db) == 0

        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 1
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 0
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


def _setup_delimited_file(sdc_executor, tmp_directory, csv_records):
    """Setup csv records and save in local system. The pipelines looks like:

            dev_raw_data_source >> local_fs

    """
    raw_data = "\n".join(csv_records)
    pipeline_builder = sdc_executor.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT',
                            directory_template=tmp_directory,
                            files_prefix='sdc-${sdc:id()}', files_suffix='csv')

    dev_raw_data_source >> local_fs
    files_pipeline = pipeline_builder.build('Generate files pipeline')
    sdc_executor.add_pipeline(files_pipeline)

    # Generate some batches/files.
    sdc_executor.start_pipeline(files_pipeline).wait_for_finished(timeout_sec=5)

    return csv_records


# SDC-13556: Do not spin JDBC Destination and Tee Processor machinery for empty batches
@sdc_min_version('3.14.0')
@database('mysql')
@pytest.mark.parametrize('use_multi_row', [True, False])
def test_jdbc_tee_commits_on_empty_batches(use_multi_row, sdc_builder, sdc_executor, database):
    """Ensure that the JDBC Tee processor won't generate commits on empty batches. Since it's generally difficult
    to create empty batches in SDC, we use scripting origin to generate them and then check commit timer (which also
    contains count) to ensure that we don't generate excessive commits on the database."""
    builder = sdc_builder.get_pipeline_builder()
    table_name = get_random_string(string.ascii_lowercase, 20)

    script = """
// First batch contains exactly one record
var batch = sdc.createBatch();
var record = sdc.createRecord('generated data');
record.value = {'name': 'A'};
batch.add(record);
batch.process("batch", "non-empty");

// Sent 1000 batches that will be empty
var step;
for (step = 0; step < 1000; step++) {
  batch = sdc.createBatch();
  batch.process("whatever", "batch-" + step);
}
"""

    origin = builder.add_stage('JavaScript Scripting')
    origin.record_type='NATIVE_OBJECTS'
    origin.user_script=script

    tee = builder.add_stage('JDBC Tee')
    tee.default_operation = 'INSERT'
    tee.field_to_column_mapping = [dict(columnName='name', field='/name', paramValue='?')]
    tee.generated_column_mappings = [dict(columnName='id', field='/id')]
    tee.table_name = table_name
    tee.use_multi_row_operation = use_multi_row

    trash = builder.add_stage('Trash')

    origin >> tee >> trash

    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = _create_table(table_name, database)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # First of all, verify that the table have exactly one record with expected values
        result = database.engine.execute(table.select())
        db = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert len(db) == 1
        assert db[0][0] == 'A'
        assert db[0][1] == 1

        # Second of all, we should see exactly 1001 batches generated by our scripting origin
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchCount.counter').count == 1001

        # Then let's explore how many commits have we generated to ensure that we don't have 1001 commits
        expected_commits = 1 if use_multi_row else 2
        assert history.latest.metrics.timer('custom.JDBCTee_01.Commit Timer.0.timer').count == expected_commits
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
@pytest.mark.parametrize('dyn_table', [False, True])
@pytest.mark.parametrize('multi_row', [False, True])
def test_error_handling_when_there_is_no_primary_key(sdc_builder, sdc_executor, database, dyn_table, multi_row):
    """
    SDC-12960. Updating JDBC table with no PK results in NPE

    Tests if a user friendly error will be added to record errors, one error for each failing record,
    instead of breaking the whole pipeline when UPDATE or DELETE operation is used and a destination table has no
    primary key.
    Covers cases with/without dynamic tables, with/without multi row parameter enabled.

    The pipeline:
    Dev Raw Data Source->Expression Evaluator->Field Remover->JDBC Producer

    The data source will send 2 insert records, 1 update and 1 delete record.
    The records also contain fields that define what table (if the dyamic table testing is enabled) to use;
    and what operation to perform on the data.

    The evaluator will use the control fields to set
    the sdc.operation.type attribute which defines what operation to perform;
    the tbl attribute which will be used in an EL to specify the destination table.

    The remover will remove the control fields from the records.

    And the producer does the actual data writing of records using their control attributes set by the evaluator.

    If the dynamic testing is enabled then the destination table name is defined by the record itself,
    this allows in one test to write data to different tables;
    if disabled, then all records will be send to the same destination table.

    If multi row testing is enabled, then one SQL query may contain data of several rows.

    We expect that if a destination table has no PRIMARY KEY, the delete and the update operations fail.
    That should not break the whole pipeline.
    Only the failed record should be marked as failed.
    And the error message should contain a description of the actual error and not the NPE as before.
    """

    table_names = [get_random_string(string.ascii_lowercase, 20) for _ in range(0, 4 if dyn_table else 1)]
    table_name_expression = ("${str:toUpper(record:attribute('tbl'))}" 
        if database.type == 'Oracle'
        else "${record:attribute('tbl')}") if dyn_table else (
        table_names[0].upper() if database.type == 'Oracle'
        else table_names[0])
    metadata = sqlalchemy.MetaData()
    tables = [sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=False, quote=True, autoincrement=False),
        sqlalchemy.Column('operation', sqlalchemy.String(32), quote=True),
    ) for table_name in table_names]
    expected_error_record_count = 3 * len(tables)
    builder = sdc_builder.get_pipeline_builder()

    data_source = builder.add_stage('Dev Raw Data Source')
    data_source.data_format = 'JSON'
    data_source.stop_after_first_batch = True
    data_source.raw_data = '\n'.join(['\n'.join(json.dumps(obj) for obj in [{
        "id": 1,
        "operation_header": "2",
        "operation": "delete",
        "table": table_name
    }, {
        "id": 2,
        "operation_header": "3",
        "operation": "update",
        "table": table_name
    }, {
        "id": 3,
        "operation_header": "3",
        "operation": "update",
        "table": table_name
    }]) for table_name in table_names])

    expression = builder.add_stage('Expression Evaluator')
    expression.field_expressions = []
    expression.header_attribute_expressions = [{
        "attributeToSet": "sdc.operation.type",
        "headerAttributeExpression": "${record:value('/operation_header')}"
    }, {
        "attributeToSet": "tbl",
        "headerAttributeExpression": "${record:value('/table')}"
    }]

    remover = builder.add_stage('Field Remover')
    remover.set_attributes(fields=['/operation_header', '/table'], action='REMOVE')

    producer = builder.add_stage('JDBC Producer')
    producer.field_to_column_mapping = []
    producer.default_operation = 'UPDATE'
    producer.table_name = table_name_expression
    producer.use_multi_row_operation = multi_row
    if database.type == 'Oracle':
        producer.enclose_object_names = True

    data_source >> expression >> remover >> producer

    pipeline = builder.build().configure_for_environment(database)

    pipeline.stages.get(label=producer.label).table_name = table_name_expression

    sdc_executor.add_pipeline(pipeline)

    created_table_count = 0  # We will remember how many tables we have actually succeeded to create
    connection = database.engine.connect()
    try:
        for table in tables:
            # If for some crazy reason this fails
            # created_table_count will contain the number of tables we actually have succeeded to create
            table.create(database.engine)
            created_table_count += 1
            connection.execute(table.insert(), [
                {'id': 1, 'operation': 'insert'},
                {'id': 2, 'operation': 'insert'},
                {'id': 3, 'operation': 'insert'}
            ])

        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline,
                                                 start_pipeline=True).snapshot
        logger.info('pipeline: %s', pipeline)
        sdc_executor.get_pipeline_status(pipeline).wait_for_status('FINISHED')
        stage = snapshot[producer.instance_name]

        assert expected_error_record_count == len(stage.error_records)
        for i in range(0, expected_error_record_count):
            assert 'JDBC_62' == stage.error_records[i].header['errorCode']
    finally:
        for i in range(0, created_table_count):  # We will drop only the tables we have created
            tables[i].drop(database.engine)
        connection.close()