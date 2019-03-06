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
import json
import logging
import math
import random
import string
import time

import pytest
import sqlalchemy
from streamsets.testframework.environments.databases import Oracle, SQLServerDatabase
from streamsets.testframework.markers import database, sdc_min_version
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


@database
def test_jdbc_multitable_consumer_origin_simple(sdc_builder, sdc_executor, database):
    """
    Check if Jdbc Multi-table Origin can retrieve any records from a table.
    Destination is Trash.
    Verify input and output (via snapshot).
    """
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, 20))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[{"tablePattern": f'%{src_table_prefix}%'}])

    trash = pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32))
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # Column names are converted to lower case since Oracle database column names are in upper case.
        rows_from_snapshot = [{record.value['value'][1]['sqpath'].lstrip('/').lower():
                               record.value['value'][1]['value']}
                              for record in snapshot[pipeline[0].instance_name].output]

        assert rows_from_snapshot == [{'name': row['name']} for row in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_query_no_more_data(sdc_builder, sdc_executor, database):
    """
    This test case uses the JDBC Query origin.
    Test that Pipeline Finisher works.
    """
    metadata = sqlalchemy.MetaData()
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32))
    )

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_query_consumer = pipeline_builder.add_stage('JDBC Query Consumer')
    jdbc_query_consumer.configuration['isIncrementalMode'] = False
    jdbc_query_consumer.sql_query = 'SELECT * FROM {0}'.format(table_name)

    trash = pipeline_builder.add_stage('Trash')
    jdbc_query_consumer >> trash

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    jdbc_query_consumer >= finisher

    try:
        logger.info('Jdbc No More Data: Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

    finally:
        logger.info('Jdbc No More Data: Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_multitable_consumer_with_finisher(sdc_builder, sdc_executor, database):
    """
    Test reading with Multi-table JDBC, output to trash.
    Test some table names that start with numbers (SDC-5381).
    Check if Pipeline Finished Executor works correctly.
    """
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[{"tablePattern": f'%{src_table_prefix}%'}])
    finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    trash = pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >= finisher
    jdbc_multitable_consumer >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    random.seed()

    tables = []
    metadata = sqlalchemy.MetaData()
    try:
        connection = database.engine.connect()

        num_letters = 10
        num_recs = 10
        num_tables = 3
        for i in range(0, num_tables):
            if i % 2 == 1:
                # table name starts with a number, contains mixed-case letters.
                input_name = '{}_{}_{}'.format(str(i), src_table_prefix,
                                               get_random_string(string.ascii_lowercase, num_letters))
            else:
                # table name comprised of mixed-case letters only.
                input_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, num_letters))

            tables.append(sqlalchemy.Table(
                input_name,
                metadata,
                sqlalchemy.Column('serial', sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column('data', sqlalchemy.Integer)
            ))
            tables[i].create(database.engine)

            rows = [{'serial': j, 'data': random.randint(0, 2100000000)} for j in range(1, num_recs + 1)]
            connection.execute(tables[i].insert(), rows)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
    finally:
        for table in tables:
            table.drop(database.engine)


@database
@sdc_min_version('3.2.0')
def test_jdbc_multitable_consumer_with_no_more_data_event_generation_delay(sdc_builder, sdc_executor, database):
    """
    Test reading Multi-table JDBC produced event to trash and output to trash.
    Test when the no more data event generation delay is configured,
    check if there is a run away thread while finishing the pipeline (SDC-11009)
    """
    src_table = get_random_string(string.ascii_lowercase, 6)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(no_more_data_event_generation_delay_in_seconds=10,
        table_configs=[{"tablePattern": f'%{src_table}%'}])

    trash = pipeline_builder.add_stage('Trash')
    trash_event = pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >= trash
    jdbc_multitable_consumer >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    random.seed()

    metadata = sqlalchemy.MetaData()
    try:
        connection = database.engine.connect()

        table = sqlalchemy.Table(
                src_table,
                metadata,
                sqlalchemy.Column('serial', sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column('data', sqlalchemy.Integer)
            )
        table.create(database.engine)

        rows = [{'serial': 1, 'data': random.randint(0, 2100000000)}]
        connection.execute(table.insert(), rows)

        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline,
                                                 start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        current_status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        assert current_status == 'STOPPED'

        event_record = snapshot[jdbc_multitable_consumer.instance_name].event_records
        assert len[event_record] == 1
    finally:
        if table is not None:
            table.drop(database.engine)


def _get_random_name(database, prefix='', length=5):
    """Generate a random string to use as a database object name.

    It handles letter case according to the database type, forcing upper-case (e.g. Oracle) or lower-case
    (e.g. Postgres).

    Args:
        database: a :obj:`streamsets.testframework.environment.Database` object.
        prefix: (:obj:`str`) add a prefix to the generated name. Default: ''.
        length: (:obj:`int`) number of characters of the generated name (without counting ``prefix``).

    """
    if type(database) == Oracle:
        name = '{}{}'.format(prefix.upper(), get_random_string(string.ascii_uppercase))
    else:
        name = '{}{}'.format(prefix.lower(), get_random_string(string.ascii_lowercase))

    return name


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
    if type(database) == Oracle:
        database.engine.execute('CREATE USER {user} IDENTIFIED BY {pwd}'.format(user=schema_name, pwd=schema_name))
        database.engine.execute('GRANT UNLIMITED TABLESPACE TO {user}'.format(user=schema_name))
    else:
        schema = sqlalchemy.schema.CreateSchema(schema_name)
        database.engine.execute(schema)


def _drop_schema(schema_name, database):
    """Remove a schema from the given database.

    Args:
        schema_name: (:obj:`str`) name of the schema to remove.
        database: a :obj:`streamsets.testframework.environment.Database` object.

    """
    if type(database) == Oracle:
        database.engine.execute('DROP USER {user}'.format(user=schema_name))
    else:
        sqlalchemy.schema.DropSchema(schema_name)


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
        rows_from_snapshot = [{record.value['value'][1]['sqpath'].lstrip('/'): record.value['value'][1]['value']}
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
    if type(database) == Oracle:
        pytest.skip('JDBC Tee Processor does not support Oracle')
    elif type(database) == SQLServerDatabase:
        pytest.skip('JDBC Tee Processor does not support multi row op on SQL Server')

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
        rows_from_snapshot = [{item.value['value'][0]['sqpath'].strip('/'): item.value['value'][0]['value'],
                               item.value['value'][1]['sqpath'].strip('/'): int(item.value['value'][1]['value'])}
                              for item in snapshot[jdbc_tee].output]
        assert rows_from_snapshot == ROWS_IN_DATABASE
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database
@pytest.mark.parametrize('use_multi_row', [True, False])
@sdc_min_version('3.0.0.0') #stop_after_first_batch
def test_jdbc_tee_processor_multi_ops(sdc_builder, sdc_executor, database, use_multi_row):
    """JDBC Tee processor with multiple operations
    Pipeline will delete/update/insert records into database with one batch and then update 'id'
    field if it is inserted. The 'operation' field is used for the record header sdc.operation.type
    which defines the CRUD operation (1: Insert, 2: Delete, 3: Update). The pipeline looks like:
        dev_raw_data_source >> expression evaluator >> jdbc_tee >> trash
    """
    if type(database) == Oracle:
        pytest.skip('JDBC Tee Processor does not support Oracle')

    if use_multi_row == True and type(database) == SQLServerDatabase:
        pytest.skip('JDBC Tee Processor does not support multi row on SQL Server')

    table_name = get_random_string(string.ascii_lowercase, 20)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    DATA = [
        {'operation': 2, 'name': 'Jarcec', 'id': 2}, # delete
        {'operation': 3, 'name': 'Hari', 'id': 3}, # update
        {'operation': 1, 'name': 'Eddie'} # insert, id will be added by JDBC Tee
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
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1]) # order by id
        result.close()
        expected_data = [(row['name'], row['id']) for row in ROWS_IN_DATABASE]
        for record in DATA:
            if record['operation'] == 1: # insert
                sequence_id += 1
                expected_data.append((record['name'], sequence_id))
            elif record['operation'] == 2: # delete
                expected_data = [row for row in expected_data if row[1] != record['id']]
            elif record['operation'] == 3: # update
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

    jdbc_query_executor.set_attributes(sql_query=query_str)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> record_deduplicator >> jdbc_query_executor
    record_deduplicator >> trash
    pipeline = pipeline_builder.build(title='JDBC Query Executor').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(RAW_DATA) - 1)
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1]) # order by id
        result.close()
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


def _create_jdbc_producer_pipeline(pipeline_builder, pipeline_title, raw_data, table_name, operation,
                                  max_cache_size_per_batch_in_entries=-1):
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
                                 stage_on_record_error='STOP_PIPELINE',
                                 max_cache_size_per_batch_in_entries=max_cache_size_per_batch_in_entries)

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
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1]) # order by id
        result.close()
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE]
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
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1]) # order by id
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
        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE if record['id'] not in removed_ids]
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
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1]) # order by id
        result.close()
        updated_names = {record['id']: record['name'] for record in ROWS_IN_DATABASE}
        updated_names.update({record['id']: record['name'] for record in ROWS_TO_UPDATE})
        assert data_from_database == [(updated_names[record['id']], record['id']) for record in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)

@pytest.mark.parametrize('cache_size', [-1, 1])
@database
def test_jdbc_producer_cache_size(sdc_builder, sdc_executor, cache_size, database):
    """Try various cache sizes on update (that leads to a different queries)."""
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32)),
        sqlalchemy.Column('nickname', sqlalchemy.String(32))
    )
    table.create(database.engine)

    INITIAL = [
        {'id': 1, 'name': 'Dima', 'nickname': 'scholar'},
        {'id': 2, 'name': 'Jarcec', 'nickname': 'troll'},
        {'id': 3, 'name': 'Arvind', 'nickname': 'boss'}
    ]

    logger.info('Adding %s rows into %s database ...', len(INITIAL), database.type)
    connection = database.engine.connect()
    connection.execute(table.insert(), INITIAL)

    UPDATE = [
        {'id': 2, 'name': 'Eddie'},
        {'id': 3, 'nickname': 'dictator'}
    ]
    DATA = '\n'.join(json.dumps(rec) for rec in UPDATE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    pipeline = _create_jdbc_producer_pipeline(pipeline_builder, 'JDBC Producer Update', DATA, table_name, 'UPDATE',
                                             max_cache_size_per_batch_in_entries=cache_size)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(UPDATE))
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table.select())
        data = sorted(result.fetchall(), key=lambda row: row[0]) # order by id
        result.close()

        assert data[1][1] == 'Eddie'
        assert data[2][2] == 'dictator'

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)

# SDC-10987: JDBC Multitable Consumer multiple offset columns with initial offset
@database
def test_jdbc_multitable_consumer_initial_offset_at_the_end(sdc_builder, sdc_executor, database):
    """
    Set initial offset at the end of the table and verify that no records were read.
    """
    table_name = get_random_string(string.ascii_lowercase, 10)

    builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.table_configs=[{
        "tablePattern": table_name,
        "overrideDefaultOffsetColumns": True,
        "offsetColumns": ["id"],
        "offsetColumnToInitialOffsetValue": [{
            "key": "id",
            "value": "5"
        }]
    }]

    trash = builder.add_stage('Trash')

    jdbc_multitable_consumer >> trash

    pipeline = builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32))
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        # Since the pipeline is not meant to read anything, we 'simply' wait
        time.sleep(5)

        sdc_executor.stop_pipeline(pipeline)

        # There must be no records read
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 0
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 0
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


# SDC-10562: Row-level stage errors not being caught at pipeline
@sdc_min_version('3.0.0.0')
@database
def test_jdbc_producer_multirow_with_duplicates(sdc_builder, sdc_executor, database):
    """
    Make sure that when using Multi Row insert, data related errors are send to error stream.
    """
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

    source >> producer

    pipeline = builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True)
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
    if type(database) == Oracle:
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
        logger.info('Dropping tables %s, %s, %s in %s database...', table1_name, table2_name, table3_name, database.type)
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
    schema1_name = _get_random_name(database, prefix='stf_schema_')
    schema2_name = _get_random_name(database, prefix='stf_schema_')
    schema3_name = _get_random_name(database, prefix='stf_schema_')
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
    schema1_name = _get_random_name(database, prefix='stf_schema_')
    schema2_name = _get_random_name(database, prefix='stf_schema_')
    schema3_name = _get_random_name(database, prefix='stf_schema_')
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
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('a', sqlalchemy.Integer),
        sqlalchemy.Column('b', sqlalchemy.Integer)
    )

    RAW_DATA = [
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

    source >> expression >> remover >> producer

    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        result = database.engine.execute(table.select())
        db = sorted(result.fetchall(), key=lambda row: row[0]) # order by id
        result.close()

        assert len(db) == 1
        assert 1 == db[0][0]
        assert 3 == db[0][1]
        assert 2 == db[0][2]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@sdc_min_version('3.0.0.0')
@database
def test_jdbc_multitable_events(sdc_builder, sdc_executor, database):
    """
    Validate that we properly generate events
    """
    table_prefix = get_random_string(string.ascii_lowercase, 20)
    table_a = '{}_a'.format(table_prefix)
    table_b = '{}_b'.format(table_prefix)
    table_events = '{}_events'.format(table_prefix)

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('JDBC Multitable Consumer')
    source.transaction_isolation = 'TRANSACTION_READ_COMMITTED'
    source.table_configs=[{
        'tablePattern': f'{table_prefix}%',
        'tableExclusionPattern': table_events
    }]

    trash = builder.add_stage('Trash')

    expression = builder.add_stage('Expression Evaluator')
    expression.field_expressions = [ {
        'fieldToSet' : '/tbl',
        'expression' : '${record:value("/table")}${record:value("/tables[0]")}'
      }, {
        'fieldToSet' : '/tbls',
        'expression' : '${record:value("/tables[0]")},${record:value("/tables[1]")}'
      }, {
        'fieldToSet' : '/event',
        'expression' : '${record:eventType()}'
      }
    ]

    producer = builder.add_stage('JDBC Producer')
    producer.table_name = table_events
    producer.default_operation = 'INSERT'
    producer.field_to_column_mapping = [
        dict(field='/event', columnName='event'),
        dict(field='/tbl', columnName='tbl'),
        dict(field='/tbls', columnName='tbls')
    ]

    source >> trash
    source >= expression
    expression >> producer

    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    #  We need three tables for this test
    metadata = sqlalchemy.MetaData()
    a = sqlalchemy.Table(
        table_a,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True)
    )
    b = sqlalchemy.Table(
        table_b,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True)
    )
    events = sqlalchemy.Table(
        table_events,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('event', sqlalchemy.String(50)),
        sqlalchemy.Column('tbl', sqlalchemy.String(150)),
        sqlalchemy.Column('tbls', sqlalchemy.String(150))
    )
    try:
        logger.info('Creating tables %s, %s and %s in %s database ...', table_a, table_b, table_events, database.type)
        a.create(database.engine)
        b.create(database.engine)
        events.create(database.engine)

        logger.info('Inserting rows into %s and %s', table_a, table_b)
        connection = database.engine.connect()
        connection.execute(a.insert(), {'id': 1})
        connection.execute(b.insert(), {'id': 1})

        # Start the pipeline
        status = sdc_executor.start_pipeline(pipeline)

        # Read two records, generate 4 events, 6 records
        status.wait_for_pipeline_output_records_count(6)

        result = database.engine.execute(events.select())
        db = sorted(result.fetchall(), key=lambda row: row[0]) # order by stamp
        result.close()
        assert len(db) == 4

        tbls = set()
        assert 'table-finished' == db[0][1]
        tbls.add(db[0][2])
        assert 'table-finished' == db[1][1]
        tbls.add(db[1][2])
        assert table_a in tbls
        assert table_b in tbls

        assert 'schema-finished' == db[2][1]
        tbls = set(db[2][3].split(","))
        assert table_a in tbls
        assert table_b in tbls

        assert 'no-more-data' == db[3][1]

        # Portable truncate
        events.drop(database.engine)
        events.create(database.engine)

        # Second iteration - insert one new row
        logger.info('Inserting rows into %s', table_a)
        connection = database.engine.connect()
        connection.execute(a.insert(), {'id': 2})

        # 1 record, 3 events more
        status.wait_for_pipeline_output_records_count(10)

        result = database.engine.execute(events.select())
        db = sorted(result.fetchall(), key=lambda row: row[0]) # order by stamp
        result.close()
        assert len(db) == 3

        assert 'table-finished' == db[0][1]
        assert table_a == db[0][2]

        assert 'schema-finished' == db[1][1]
        tbls = set(db[1][3].split(","))
        assert table_a in tbls
        assert table_b in tbls

        assert 'no-more-data' == db[2][1]
    finally:
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Dropping tables %s, %s and %s in %s database...', table_a, table_b, table_events, database.type)
        a.drop(database.engine)
        b.drop(database.engine)
        events.drop(database.engine)
