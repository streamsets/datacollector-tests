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
import os
import random
import string
import tempfile
import time
from collections import OrderedDict

import pytest
import sqlalchemy
import datetime
from streamsets.sdk.utils import Version
from streamsets.testframework.environments.databases import Db2Database, OracleDatabase, SQLServerDatabase, PostgreSqlDatabase
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
        tuples_to_lower_name = lambda tup: (tup[0].lower(), tup[1])
        rows_from_snapshot = [tuples_to_lower_name(list(record.field.items())[1])
                              for record in snapshot[pipeline[0].instance_name].output]

        assert rows_from_snapshot == [('name', row['name']) for row in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_consumer_offset_resume(sdc_builder, sdc_executor, database):
    """Ensure that the Query consumer can resume where it ended and stop the pipeline when it reads all the data."""
    if isinstance(database, OracleDatabase):
        pytest.skip('This test does not support oracle and its upper casing of column names.')

    metadata = sqlalchemy.MetaData()
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32))
    )

    pipeline_builder = sdc_builder.get_pipeline_builder()

    origin = pipeline_builder.add_stage('JDBC Query Consumer')
    origin.incremental_mode = True
    origin.sql_query = 'SELECT * FROM {0} WHERE '.format(table_name) + 'id > ${OFFSET} ORDER BY id'
    origin.initial_offset = '0'
    origin.offset_column = 'id'

    trash = pipeline_builder.add_stage('Trash')
    origin >> trash

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    origin >= finisher

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)
        connection = database.engine.connect()

        for i in range(len(ROWS_IN_DATABASE)):
            # Insert one row to the database
            connection.execute(table.insert(), [ROWS_IN_DATABASE[i]])

            snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
            assert len(snapshot[origin].output) == 1
            assert snapshot[origin].output[0].get_field_data('/id') == i + 1

            # TLKT-249: Add wait_for_finished to get_status object
            sdc_executor.get_pipeline_status(pipeline).wait_for_status('FINISHED')
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_consumer_non_incremental_mode(sdc_builder, sdc_executor, database):
    """Ensure that the Query consumer works properly in non-incremental mode."""
    if database.type == 'Oracle':
        pytest.skip("This test depends on proper case for column names that Oracle auto-uppers.")

    metadata = sqlalchemy.MetaData()
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32))
    )

    pipeline_builder = sdc_builder.get_pipeline_builder()

    origin = pipeline_builder.add_stage('JDBC Query Consumer')
    origin.incremental_mode = False
    origin.sql_query = 'SELECT * FROM {0}'.format(table_name)

    trash = pipeline_builder.add_stage('Trash')
    origin >> trash

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    origin >= finisher

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)
        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        # Run the pipeline N times, it should always read the same
        for i in range(3):
            snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
            assert len(snapshot[origin].output) == len(ROWS_IN_DATABASE)

            assert snapshot[origin].output[0].get_field_data('/id') == 1
            assert snapshot[origin].output[1].get_field_data('/id') == 2
            assert snapshot[origin].output[2].get_field_data('/id') == 3

            # TLKT-249: Add wait_for_finished to get_status object
            sdc_executor.get_pipeline_status(pipeline).wait_for_status('FINISHED')
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


# SDC-11009: Run away pipeline runners in JDBC Multithread origins when no-more-data generation delay is configured
@database
@sdc_min_version('3.2.0')
def test_jdbc_multitable_consumer_with_no_more_data_event_generation_delay(sdc_builder, sdc_executor, database):
    """
    Make sure that when a delayed no-more-data is being processed, the pipeline properly waits on the processing to
    finish before stopping.

    source >> trash
           >= delay (only for no-more-data) >> trash
    """
    src_table = get_random_string(string.ascii_lowercase, 6)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.no_more_data_event_generation_delay_in_seconds = 1
    jdbc_multitable_consumer.table_configs = [{"tablePattern": f'%{src_table}%'}]

    trash = pipeline_builder.add_stage('Trash')

    delay = pipeline_builder.add_stage('Delay')
    delay.delay_between_batches = 10 * 1000
    delay.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']

    trash_event = pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >> trash
    jdbc_multitable_consumer >= delay
    delay >> trash_event

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    metadata = sqlalchemy.MetaData()
    try:
        connection = database.engine.connect()

        table = sqlalchemy.Table(
            src_table,
            metadata,
            sqlalchemy.Column('serial', sqlalchemy.Integer, primary_key=True)
        )
        table.create(database.engine)

        rows = [{'serial': 1}]
        connection.execute(table.insert(), rows)

        # We start the pipeline
        sdc_executor.start_pipeline(pipeline)

        # We wait three seconds - one second for the no-more-data to be generated and then some buffer time
        time.sleep(3)

        # Then we try to stop the pipeline, now the pipeline should not stop immediately and should in-fact wait
        sdc_executor.stop_pipeline(pipeline).wait_for_stopped()
        current_status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        assert current_status == 'STOPPED'

        # Validate expected metrics
        history = sdc_executor.get_pipeline_history(pipeline)
        # Total number of input records
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1
        # 1 record, 1 no-more-data (rest of events is discarded)
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 2
        # The table itself contained only one record
        assert history.latest.metrics.counter('stage.Trash_01.inputRecords.counter').count == 1
        # Only no-more-data event should reach the destination
        assert history.latest.metrics.counter('stage.Trash_02.inputRecords.counter').count == 1
        # The max batch time should be slightly more then 10 (the delayed batch that we have caused)
        # TODO: TLKT-167: Add access methods to metric objects
        assert history.latest.metrics.timer('pipeline.batchProcessing.timer')._data.get('max') >= 10
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
    if isinstance(database, OracleDatabase):
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
    if isinstance(database, OracleDatabase):
        database.engine.execute('CREATE USER {user} IDENTIFIED BY {pwd}'.format(user=schema_name, pwd=schema_name))
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


# SDC-10987: JDBC Multitable Consumer multiple offset columns with initial offset
@database
def test_jdbc_multitable_consumer_initial_offset_at_the_end(sdc_builder, sdc_executor, database):
    """
    Set initial offset at the end of the table and verify that no records were read.
    """
    table_name = get_random_string(string.ascii_lowercase, 10)

    builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.table_configs = [{
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
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        sqlalchemy.Column('name', sqlalchemy.String(32), quote=True)
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


@sdc_min_version('3.0.0.0')
@database
def test_jdbc_multitable_events(sdc_builder, sdc_executor, database):
    """
    Validate that we properly generate events
    """
    if database.type == 'Oracle':
        pytest.skip("This test depends on auto-created ID that doesn't work properly on Oracle")

    table_prefix = get_random_string(string.ascii_lowercase, 20)
    table_a = '{}_a'.format(table_prefix)
    table_b = '{}_b'.format(table_prefix)
    table_events = '{}_events'.format(table_prefix)

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('JDBC Multitable Consumer')
    source.transaction_isolation = 'TRANSACTION_READ_COMMITTED'
    source.table_configs = [{
        'tablePattern': f'{table_prefix}%',
        "enableNonIncremental": True,
        'tableExclusionPattern': table_events
    }]

    trash = builder.add_stage('Trash')

    expression = builder.add_stage('Expression Evaluator')
    expression.field_expressions = [{
        'fieldToSet': '/tbl',
        'expression': '${record:value("/table")}${record:value("/tables[0]")}'
    }, {
        'fieldToSet': '/tbls',
        'expression': '${record:value("/tables[0]")},${record:value("/tables[1]")}'
    }, {
        'fieldToSet': '/event',
        'expression': '${record:eventType()}'
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
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=False)
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
        db = sorted(result.fetchall(), key=lambda row: row[0])  # order by stamp
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
        db = sorted(result.fetchall(), key=lambda row: row[0])  # order by stamp
        result.close()
        assert len(db) == 3

        assert 'table-finished' == db[0][1]
        assert table_a == db[0][2]

        assert 'schema-finished' == db[1][1]
        tbls = set(db[1][3].split(","))
        assert table_a in tbls
        assert table_b in tbls

        assert 'no-more-data' == db[2][1]

        # Now let's stop the pipeline and start it again
        # SDC-10022: Multitable JDBC Origin with non-incremental table does not properly trigger 'no-more-data' event
        sdc_executor.stop_pipeline(pipeline)

        # Portable truncate
        events.drop(database.engine)
        events.create(database.engine)

        # Start the pipeline and wait for it to read three records (3 events)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(3)

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


# SDC-11082: Extend support for TIMESTAMP WITH TIMEZONE Datatypes
@sdc_min_version('3.0.0.0')
@database('oracle')
# https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1821
# We don't support UriType (requires difficult workaround in JDBC)
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', [
    ('number', '1', 'DECIMAL', '1'),
    ('char(2)', "'AB'", 'STRING', 'AB'),
    ('varchar(4)', "'ABCD'", 'STRING', 'ABCD'),
    ('varchar2(4)', "'NVAR'", 'STRING', 'NVAR'),
    ('nchar(3)', "'NCH'", 'STRING', 'NCH'),
    ('nvarchar2(4)', "'NVAR'", 'STRING', 'NVAR'),
    ('binary_float', '1.0', 'FLOAT', '1.0'),
    ('binary_double', '2.0', 'DOUBLE', '2.0'),
    ('date', "TO_DATE('1998-1-1 6:22:33', 'YYYY-MM-DD HH24:MI:SS')", 'DATETIME', 883635753000),
    ('timestamp', "TIMESTAMP'1998-1-2 6:00:00'", 'DATETIME', 883720800000),
    ('timestamp with time zone', "TIMESTAMP'1998-1-3 6:00:00-5:00'", 'ZONED_DATETIME', '1998-01-03T06:00:00-05:00'),
    ('timestamp with local time zone', "TIMESTAMP'1998-1-4 6:00:00-5:00'", 'ZONED_DATETIME', '1998-01-04T11:00:00Z'),
    ('long', "'LONG'", 'STRING', 'LONG'),
    ('blob', "utl_raw.cast_to_raw('BLOB')", 'BYTE_ARRAY', 'QkxPQg=='),
    ('clob', "'CLOB'", 'STRING', 'CLOB'),
    ('nclob', "'NCLOB'", 'STRING', 'NCLOB'),
    ('XMLType', "xmltype('<a></a>')", 'STRING', '<a></a>')
])
@pytest.mark.parametrize('use_table_origin', [True, False])
def test_jdbc_multitable_oracle_types(sdc_builder, sdc_executor, database, use_table_origin, sql_type, insert_fragment,
                                      expected_type, expected_value):
    """Test all feasible Oracle types."""
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name}(
                id number primary key,
                data_column {sql_type} NULL
            )
        """)

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        # And a null
        connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")

        builder = sdc_builder.get_pipeline_builder()

        if use_table_origin:
            origin = builder.add_stage('JDBC Multitable Consumer')
            origin.table_configs = [{"tablePattern": f'%{table_name}%'}]
            origin.on_unknown_type = 'CONVERT_TO_STRING'
        else:
            origin = builder.add_stage('JDBC Query Consumer')
            origin.sql_query = 'SELECT * FROM {0}'.format(table_name)
            origin.incremental_mode = False
            origin.on_unknown_type = 'CONVERT_TO_STRING'

        trash = builder.add_stage('Trash')

        origin >> trash

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert len(snapshot[origin].output) == 2
        record = snapshot[origin].output[0]
        null_record = snapshot[origin].output[1]

        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.
        # TLKT-177: Add ability for field to return raw value

        assert record.field['DATA_COLUMN'].type == expected_type
        assert null_record.field['DATA_COLUMN'].type == expected_type

        assert record.field['DATA_COLUMN']._data['value'] == expected_value
        assert null_record.field['DATA_COLUMN'] == None
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


# SDC-11324: JDBC MultiTable origin can create duplicate offsets
@database('mysql')
def test_jdbc_multitable_duplicate_offsets(sdc_builder, sdc_executor, database):
    """Validate that we will not create duplicate offsets. """
    table_name = get_random_string(string.ascii_lowercase, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    origin = pipeline_builder.add_stage('JDBC Multitable Consumer')
    origin.table_configs = [{"tablePattern": table_name}]
    origin.max_batch_size_in_records = 1

    trash = pipeline_builder.add_stage('Trash')

    origin >> trash

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
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_IN_DATABASE))
        sdc_executor.stop_pipeline(pipeline)

        # We should have transition 4 records
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == len(ROWS_IN_DATABASE)
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == len(ROWS_IN_DATABASE)

        # And most importantly, validate offset
        offset = sdc_executor.api_client.get_pipeline_committed_offsets(pipeline.id).response.json()
        assert offset is not None
        assert offset['offsets'] is not None
        expected_offset = {
            f"tableName={table_name};;;partitioned=false;;;partitionSequence=-1;;;partitionStartOffsets=;;;partitionMaxOffsets=;;;usingNonIncrementalLoad=false": "id=3",
            "$com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcSource.offset.version$": "2"
        }
        assert offset['offsets'] == expected_offset
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


# SDC-11326: JDBC MultiTable origin forgets offset of non-incremental table on consecutive execution
@database('mysql')
@sdc_min_version('3.0.0.0')
def test_jdbc_multitable_lost_nonincremental_offset(sdc_builder, sdc_executor, database):
    """Validate the origin does not loose non-incremental offset on various runs."""
    table_name = get_random_string(string.ascii_lowercase, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    origin = pipeline_builder.add_stage('JDBC Multitable Consumer')
    origin.table_configs = [{"tablePattern": table_name, "enableNonIncremental": True}]
    origin.max_batch_size_in_records = 1

    trash = pipeline_builder.add_stage('Trash')

    origin >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=False),
        sqlalchemy.Column('name', sqlalchemy.String(32))
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_IN_DATABASE))
        sdc_executor.stop_pipeline(pipeline)

        # We should have read all the records
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == len(ROWS_IN_DATABASE)
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == len(ROWS_IN_DATABASE)

        # And most importantly, validate offset
        offset = sdc_executor.api_client.get_pipeline_committed_offsets(pipeline.id).response.json()
        assert offset is not None
        assert offset['offsets'] is not None
        expected_offset = {
            f"tableName={table_name};;;partitioned=false;;;partitionSequence=-1;;;partitionStartOffsets=;;;partitionMaxOffsets=;;;usingNonIncrementalLoad=true": "completed=true",
            "$com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcSource.offset.version$": "2"
        }
        assert offset['offsets'] == expected_offset

        for _ in range(5):
            sdc_executor.start_pipeline(pipeline)

            # Since the pipeline won't read anything, give it few seconds to "idle"
            time.sleep(2)
            sdc_executor.stop_pipeline(pipeline)

            # And it really should not have read anything!
            history = sdc_executor.get_pipeline_history(pipeline)
            assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 0
            assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 0

            # And offset should not have changed
            offset = sdc_executor.api_client.get_pipeline_committed_offsets(pipeline.id).response.json()
            assert offset is not None
            assert offset['offsets'] is not None
            assert offset['offsets'] == expected_offset

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@sdc_min_version('3.9.0')
@database('oracle')
def test_jdbc_multitable_oracle_split_by_timestamp_with_timezone(sdc_builder, sdc_executor, database):
    """Make sure that we can properly partition TIMESTAMP WITH TIMEZONE type."""
    table_name = get_random_string(string.ascii_uppercase, 20)
    table_name_dest = get_random_string(string.ascii_uppercase, 20)

    connection = database.engine.connect()

    comparing_query = f"""(
        select * from {table_name}
        minus
        select * from {table_name_dest}
    ) union (
        select * from {table_name_dest}
        minus
        select * from {table_name}
    )"""

    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name}(
                ID number primary key,
                TZ timestamp(6) with time zone
            )
        """)
        # Create destination table
        connection.execute(f"""CREATE TABLE {table_name_dest} AS SELECT * FROM {table_name} WHERE 1=0""")

        # Insert a few rows
        for m in range(0, 5):
            for s in range(0, 59):
                connection.execute(f"INSERT INTO {table_name} VALUES({m*100+s}, TIMESTAMP'2019-01-01 10:{m}:{s}-5:00')")
        connection.execute("commit")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('JDBC Multitable Consumer')
        origin.table_configs = [{
            "tablePattern": f'%{table_name}%',
            "overrideDefaultOffsetColumns": True,
            "offsetColumns": ["TZ"],
            "enableNonIncremental": False,
            "partitioningMode": "REQUIRED",
            "partitionSize": "30",
            "maxNumActivePartitions": -1
        }]
        origin.number_of_threads = 2
        origin.maximum_pool_size = 2
        origin.max_batch_size_in_records = 30

        finisher = builder.add_stage('Pipeline Finisher Executor')
        finisher.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']

        FIELD_MAPPINGS = [dict(field='/ID', columnName='ID'),
                          dict(field='/TZ', columnName='TZ')]
        destination = builder.add_stage('JDBC Producer')
        destination.set_attributes(default_operation='INSERT',
                                   table_name=table_name_dest,
                                   field_to_column_mapping=FIELD_MAPPINGS,
                                   stage_on_record_error='STOP_PIPELINE')

        origin >> destination
        origin >= finisher

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        result = [row.items() for row in connection.execute(comparing_query)]
        assert len(result) == 0

        # Insert few more rows and validate the outcome again
        for m in range(6, 8):
            for s in range(0, 59):
                connection.execute(f"INSERT INTO {table_name} VALUES({m*100+s}, TIMESTAMP'2019-01-01 10:{m}:{s}-5:00')")
        connection.execute("commit")

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        result = [row.items() for row in connection.execute(comparing_query)]
        assert len(result) == 0

    finally:
        logger.info('Dropping table %s and %s in %s database ...', table_name, table_name_dest, database.type)
        connection.execute(f"DROP TABLE {table_name}")
        connection.execute(f"DROP TABLE {table_name_dest}")


def _get_date_from_days(d):
    return datetime.date(1970, 1, 1) + datetime.timedelta(days=d)


@database('oracle')
def test_jdbc_multitable_oracle_split_by_date(sdc_builder, sdc_executor, database):
    """Make sure that we can properly partition DATE type.
    More precisely, we want to run this pipeline:

    multitable >> jdbc
    multitable >= finisher

    With more than one thread and using a DATE column as a offset column.
    This feature was not available until version 3.11.0, and was detected and
    solved in ESC-513.
    """
    table_name = get_random_string(string.ascii_uppercase, 20)
    table_name_dest = get_random_string(string.ascii_uppercase, 20)

    connection = database.engine.connect()

    comparing_query = f"""(
        select * from {table_name}
        minus
        select * from {table_name_dest}
    ) union (
        select * from {table_name_dest}
        minus
        select * from {table_name}
    )"""

    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name}(
                ID number primary key,
                DT date
            )
        """)
        # Create destination table
        connection.execute(f"""CREATE TABLE {table_name_dest} AS SELECT * FROM {table_name} WHERE 1=0""")

        # Insert a few rows
        for m in range(0, 5):
            for s in range(0, 59):
                identifier = 100 * m + s
                connection.execute(
                    f"INSERT INTO {table_name} VALUES({identifier}, DATE'{_get_date_from_days(identifier)}')"
                )
        connection.execute("commit")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('JDBC Multitable Consumer')
        # Partition size is set to 259200000 which corresponds to 30 days in ms,
        # since dates are translated to timestamps
        origin.table_configs = [{
            "tablePattern": f'%{table_name}%',
            "overrideDefaultOffsetColumns": True,
            "offsetColumns": ["DT"], # Should cause SDC < 3.11.0 to throw an UnsupportedOperationException
            "enableNonIncremental": False,
            "partitioningMode": "REQUIRED",
            "partitionSize": "259200000", # 30 days = 30*24*60*60*1000 (259200000)ms
            "maxNumActivePartitions": 2
        }]
        origin.number_of_threads = 2
        origin.maximum_pool_size = 2

        finisher = builder.add_stage('Pipeline Finisher Executor')
        finisher.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']

        FIELD_MAPPINGS = [dict(field='/ID', columnName='ID'),
                          dict(field='/DT', columnName='DT')]
        destination = builder.add_stage('JDBC Producer')
        destination.set_attributes(default_operation='INSERT',
                                   table_name=table_name_dest,
                                   field_to_column_mapping=FIELD_MAPPINGS,
                                   stage_on_record_error='STOP_PIPELINE')

        origin >> destination
        origin >= finisher

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        result = [row.items() for row in connection.execute(comparing_query)]
        assert len(result) == 0

        # Insert few more rows and validate the outcome again
        for m in range(6, 8):
            for s in range(0, 59):
                identifier = 100 * m + s
                connection.execute(
                    f"INSERT INTO {table_name} VALUES({identifier}, DATE'{_get_date_from_days(identifier)}')"
                )
        connection.execute("commit")

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        result = [row.items() for row in connection.execute(comparing_query)]
        assert len(result) == 0

    finally:
        logger.info('Dropping table %s and %s in %s database ...', table_name, table_name_dest, database.type)
        connection.execute(f"DROP TABLE {table_name}")
        connection.execute(f"DROP TABLE {table_name_dest}")


@sdc_min_version('3.9.0')
@database('mysql')
def test_jdbc_multitable_consumer_origin_high_resolution_timestamp_offset(sdc_builder, sdc_executor, database):
    """
    Check if Jdbc Multi-table Origin can retrieve any records from a table using as an offset a high resolution
    timestamp of milliseconds order. It is checked that the records read have a timestamp greater than the timestamp
    used as initial offset.

    Pipeline looks like:

    jdbc_multitable_consumer >> trash
    """
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = f'{src_table_prefix}_{get_random_string(string.ascii_lowercase, 20)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[{'tablePattern': f'%{src_table_prefix}%',
                                                            'overrideDefaultOffsetColumns': True,
                                                            'offsetColumns': ['added'],
                                                            'offsetColumnToInitialOffsetValue': [{
                                                                'key': 'added',
                                                                'value': '${time:extractNanosecondsFromString(' +
                                                                         '"1996-12-02 00:00:00.020111000")}'
                                                            }]
                                                            }])

    trash = pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)

    connection = database.engine.connect()
    # Create table
    logger.info('Creating table %s in %s database ...', table_name, database.type)
    connection.execute(f"""
                CREATE TABLE {table_name}(
                    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    name varchar(100) NOT NULL,
                    age INT UNSIGNED NOT NULL,
                    added TIMESTAMP(6) NOT NULL
                )
            """)

    # Insert rows
    logger.info('Adding four rows into %s database ...', database.type)
    connection.execute(f'INSERT INTO {table_name} VALUES(1, "Charly", 14, "2005-02-08 14:00:00.100105002")')
    connection.execute(f'INSERT INTO {table_name} VALUES(2, "Paco", 28, "1992-05-25 11:00:00.000201010")')
    connection.execute(f'INSERT INTO {table_name} VALUES(3, "Eugenio", 21, "1996-12-01 23:00:00.020111")')
    connection.execute(f'INSERT INTO {table_name} VALUES(4, "Romualdo", 19, "2000-06-15 18:30:00.10523121")')

    try:

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        name_id_from_output = [(record.field['name'], record.field['id'])
                               for record in snapshot[jdbc_multitable_consumer].output]

        assert len(name_id_from_output) == 2
        assert name_id_from_output == [('Romualdo', 4), ('Charly', 1)]

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f'DROP TABLE {table_name}')


@database
@sdc_min_version('3.0.0.0')
def test_jdbc_multitable_consumer_partitioned_large_offset_gaps(sdc_builder, sdc_executor, database):
    """
    Ensure that the multi-table JDBC origin can handle large gaps between offset columns in partitioned mode
    The destination is trash, and there is a finisher waiting for the no-more-data event

    The pipeline will be started, and we will capture two snapshots (to ensure all expected rows are covered),
    then assert those captured snapshot rows match the expected data.

    This is a test for SDC-10053
    """
    if database.type == 'Oracle':
        pytest.skip("This test depends on proper case for column names that Oracle auto-uppers.")

    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, 20))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[{
        "tablePattern": f'{table_name}',
        "enableNonIncremental": False,
        "partitioningMode": "REQUIRED",
        "partitionSize": "1000000",
        "maxNumActivePartitions": -1
    }])

    trash = pipeline_builder.add_stage('Trash')
    jdbc_multitable_consumer >> trash

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    finisher.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']
    jdbc_multitable_consumer >= finisher

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

        logger.info('Adding four rows into %s table, with a large gap in the primary keys ...', table_name)
        connection = database.engine.connect()
        rows_with_gap = ROWS_IN_DATABASE + [{'id': 5000000, 'name': 'Evil Jeff'}]
        connection.execute(table.insert(), rows_with_gap)
        connection.close()

        sdc_executor.add_pipeline(pipeline)
        # need to capture two batches, one for row IDs 1-3, and one for the last row after the large gap
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, batches=2, start_pipeline=True).snapshot
        rows_from_snapshot = [(record.get_field_data('/name').value, record.get_field_data('/id').value)
                              for batch in snapshot.snapshot_batches
                              for record in batch.stage_outputs[jdbc_multitable_consumer.instance_name].output]

        expected_data = [(row['name'], row['id']) for row in rows_with_gap]
        logger.info('Actual %s expected %s', rows_from_snapshot, expected_data)
        assert rows_from_snapshot == expected_data
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@sdc_min_version('3.0.0.0')
@database('mysql')
# https://dev.mysql.com/doc/refman/8.0/en/data-types.html
# We don't support BIT generally (the driver is doing funky 'random' mappings on certain versions)
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', [
    ('TINYINT', '-128', 'SHORT', -128),
    ('TINYINT UNSIGNED', '255', 'SHORT', 255),
    ('SMALLINT', '-32768', 'SHORT', -32768),
    ('SMALLINT UNSIGNED', '65535', 'SHORT', -1),  # Support for unsigned isn't entirely correct!
    ('MEDIUMINT', '-8388608', 'INTEGER', '-8388608'),
    ('MEDIUMINT UNSIGNED', '16777215', 'INTEGER', '16777215'),
    ('INT', '-2147483648', 'INTEGER', '-2147483648'),
    ('INT UNSIGNED', '4294967295', 'INTEGER', '-1'),  # Support for unsigned isn't entirely correct!
    ('BIGINT', '-9223372036854775807', 'LONG', '-9223372036854775807'),
    ('BIGINT UNSIGNED', '18446744073709551615', 'LONG', '-1'),  # Support for unsigned isn't entirely correct!
    ('DECIMAL(5, 2)', '5.20', 'DECIMAL', '5.20'),
    ('NUMERIC(5, 2)', '5.20', 'DECIMAL', '5.20'),
    ('FLOAT', '5.2', 'FLOAT', '5.2'),
    ('DOUBLE', '5.2', 'DOUBLE', '5.2'),
    #    ('BIT(8)',"b'01010101'", 'BYTE_ARRAY', 'VQ=='),
    ('DATE', "'2019-01-01'", 'DATE', 1546300800000),
    ('DATETIME', "'2019-01-01 5:00:00'", 'DATETIME', 1546318800000),
    ('TIMESTAMP', "'2019-01-01 5:00:00'", 'DATETIME', 1546318800000),
    ('TIME', "'5:00:00'", 'TIME', 18000000),
    ('YEAR', "'2019'", 'DATE', 1546300800000),
    ('CHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('VARCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('BINARY(5)', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='),
    ('VARBINARY(5)', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='),
    ('BLOB', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='),
    ('TEXT', "'Hello'", 'STRING', 'Hello'),
    ("ENUM('a', 'b')", "'a'", 'STRING', 'a'),
    ("set('a', 'b')", "'a,b'", 'STRING', 'a,b'),
    ("POINT", "POINT(1, 1)", 'BYTE_ARRAY', 'AAAAAAEBAAAAAAAAAAAA8D8AAAAAAADwPw=='),
    ("LINESTRING", "LineString(Point(0,0), Point(10,10), Point(20,25), Point(50,60))", 'BYTE_ARRAY',
     'AAAAAAECAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAkQAAAAAAAACRAAAAAAAAANEAAAAAAAAA5QAAAAAAAAElAAAAAAAAATkA='),
    ("POLYGON",
     "Polygon(LineString(Point(0,0),Point(10,0),Point(10,10),Point(0,10),Point(0,0)),LineString(Point(5,5),Point(7,5),Point(7,7),Point(5,7),Point(5,5)))",
     'BYTE_ARRAY',
     'AAAAAAEDAAAAAgAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAJEAAAAAAAAAAAAAAAAAAACRAAAAAAAAAJEAAAAAAAAAAAAAAAAAAACRAAAAAAAAAAAAAAAAAAAAAAAUAAAAAAAAAAAAUQAAAAAAAABRAAAAAAAAAHEAAAAAAAAAUQAAAAAAAABxAAAAAAAAAHEAAAAAAAAAUQAAAAAAAABxAAAAAAAAAFEAAAAAAAAAUQA=='),
    ("JSON", "'{\"a\":\"b\"}'", 'STRING', '{\"a\": \"b\"}'),
])
@pytest.mark.parametrize('use_table_origin', [True, False])
def test_jdbc_multitable_mysql_types(sdc_builder, sdc_executor, database, use_table_origin, sql_type, insert_fragment,
                                     expected_type, expected_value):
    """Test all feasible Mysql types."""
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    try:
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

        builder = sdc_builder.get_pipeline_builder()

        if use_table_origin:
            origin = builder.add_stage('JDBC Multitable Consumer')
            origin.table_configs = [{"tablePattern": f'%{table_name}%'}]
            origin.on_unknown_type = 'CONVERT_TO_STRING'
        else:
            origin = builder.add_stage('JDBC Query Consumer')
            origin.sql_query = 'SELECT * FROM {0}'.format(table_name)
            origin.incremental_mode = False
            origin.on_unknown_type = 'CONVERT_TO_STRING'

        trash = builder.add_stage('Trash')

        origin >> trash

        pipeline = builder.build(f"MySQL Type {sql_type} with value {insert_fragment}").configure_for_environment(
            database)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert len(snapshot[origin].output) == 2
        record = snapshot[origin].output[0]
        null_record = snapshot[origin].output[1]

        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.
        # TLKT-177: Add ability for field to return raw value

        assert record.field['data_column'].type == expected_type
        assert null_record.field['data_column'].type == expected_type

        assert record.field['data_column']._data['value'] == expected_value
        assert null_record.field['data_column'] == None
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.0.0.0')
@database('postgresql')
# https://www.postgresql.org/docs/11/datatype.html
# Not testing 'serial' family explicitly as that is just an alias
# Not supporting tsvector tsquery as that doesn't seem fit for us
# bit(n) is not supported
# xml is not supported
# domain types (as a category are not supported)
# pg_lsn not supported
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', [
    ('smallint', '-32768', 'SHORT', -32768),
    ('integer', '2147483647', 'INTEGER', '2147483647'),
    ('bigint', '-9223372036854775808', 'LONG', '-9223372036854775808'),
    ('decimal(5,2)', '5.20', 'DECIMAL', '5.20'),
    ('numeric(5,2)', '5.20', 'DECIMAL', '5.20'),
    ('real', '5.20', 'FLOAT', '5.2'),
    ('double precision', '5.20', 'DOUBLE', '5.2'),
    ('money', '12.34', 'DOUBLE', '12.34'),
    ('char(5)', "'Hello'", 'STRING', 'Hello'),
    ('varchar(5)', "'Hello'", 'STRING', 'Hello'),
    ('text', "'Hello'", 'STRING', 'Hello'),
    ('bytea', "'\\xDEADBEEF'", 'BYTE_ARRAY', '3q2+7w=='),
    ('timestamp', "'2003-04-12 04:05:06'", 'DATETIME', 1050120306000),
    ('timestamp with time zone', "'2003-04-12 04:05:06 America/New_York'", 'DATETIME', 1050134706000),
    # For PostgreSQL, we don't create ZONED_DATETIME
    ('date', "'2019-01-01'", 'DATE', 1546300800000),
    ('time', "'5:00:00'", 'TIME', 18000000),
    ('time with time zone', "'04:05:06-08:00'", 'TIME', 43506000),
    ('interval', "INTERVAL '1' YEAR", 'STRING', '1 years 0 mons 0 days 0 hours 0 mins 0.00 secs'),
    ('boolean', "true", 'BOOLEAN', True),
    ('ai', "'sad'", 'STRING', 'sad'),
    ('point', "'(1, 1)'", 'STRING', '(1.0,1.0)'),
    ('line', "'{1, 1, 1}'", 'STRING', '{1.0,1.0,1.0}'),
    ('lseg', "'((1,1)(2,2))'", 'STRING', '[(1.0,1.0),(2.0,2.0)]'),
    ('box', "'(1,1)(2,2)'", 'STRING', '(2.0,2.0),(1.0,1.0)'),
    ('path', "'((1,1),(2,2))'", 'STRING', '((1.0,1.0),(2.0,2.0))'),
    ('polygon', "'((1,1),(2,2))'", 'STRING', '((1.0,1.0),(2.0,2.0))'),
    ('circle', "'<(1,1),5>'", 'STRING', '<(1.0,1.0),5.0>'),
    ('inet', "'127.0.0.1/16'", 'STRING', '127.0.0.1/16'),
    ('cidr', "'127.0.0.0/16'", 'STRING', '127.0.0.0/16'),
    ('macaddr', "'08:00:2b:01:02:03'", 'STRING', '08:00:2b:01:02:03'),
    #    ('macaddr8', "'08:00:2b:01:02:03'", 'STRING', '08:00:2b:ff:fe:01:02:03'),
    #    ('bit(8)', "b'10101010'", 'BYTE_ARRAY', '08:00:2b:ff:fe:01:02:03'), # Doesn't work at all today
    ('bit varying(3)', "b'101'", 'STRING', '101'),
    ('uuid', "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", 'STRING', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
    #    ('xml', "'<foo>bar</foo>'", 'STRING', ''), # Doesn't work properly today
    ("json", "'{\"a\":\"b\"}'", 'STRING', '{"a":"b"}'),
    ("jsonb", "'{\"a\":\"b\"}'", 'STRING', '{"a": "b"}'),
    ("integer[3][3]", "'{{1,2,3},{4,5,6},{7,8,9}}'", 'STRING', '{{1,2,3},{4,5,6},{7,8,9}}'),
    ("ct", "ROW(1, 2)", 'STRING', '(1,2)'),
    ("int4range", "'[1,2)'", 'STRING', '[1,2)'),
    ("int8range", "'[1,2)'", 'STRING', '[1,2)'),
    ("numrange", "'[1,2)'", 'STRING', '[1,2)'),
    ("tsrange", "'[2010-01-01 14:30, 2010-01-01 15:30)'", 'STRING', '["2010-01-01 14:30:00","2010-01-01 15:30:00")'),
    ("tstzrange", "'[2010-01-01 14:30 America/New_York, 2010-01-01 15:30 America/New_York)'", 'STRING',
     '["2010-01-01 19:30:00+00","2010-01-01 20:30:00+00")'),
    ("daterange", "'[2010-01-01, 2010-01-02)'", 'STRING', '[2010-01-01,2010-01-02)'),
])
@pytest.mark.parametrize('use_table_origin', [True, False])
def test_jdbc_postgresql_types(sdc_builder, sdc_executor, database, use_table_origin, sql_type, insert_fragment,
                               expected_type, expected_value):
    """Test all feasible PostgreSQL types."""
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    try:
        # Create enum type conditionally
        connection.execute(f"""
            DO
            $$
            BEGIN
              IF NOT EXISTS (SELECT * FROM pg_type typ
                                      INNER JOIN pg_namespace nsp ON nsp.oid = typ.typnamespace
                                      WHERE nsp.nspname = current_schema() AND typ.typname = 'ai') THEN
                CREATE TYPE ai AS ENUM ('sad', 'ok', 'happy');
              END IF;
            END;
            $$
            LANGUAGE plpgsql;
        """)

        # Create enum complex type conditionally
        connection.execute(f"""
            DO
            $$
            BEGIN
              IF NOT EXISTS (SELECT * FROM pg_type typ
                                      INNER JOIN pg_namespace nsp ON nsp.oid = typ.typnamespace
                                      WHERE nsp.nspname = current_schema() AND typ.typname = 'ct') THEN
                CREATE TYPE ct AS (a int, b int);
              END IF;
            END;
            $$
            LANGUAGE plpgsql;
        """)

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

        builder = sdc_builder.get_pipeline_builder()

        if use_table_origin:
            origin = builder.add_stage('JDBC Multitable Consumer')
            origin.table_configs = [{"tablePattern": f'%{table_name}%'}]
            origin.on_unknown_type = 'CONVERT_TO_STRING'
        else:
            origin = builder.add_stage('JDBC Query Consumer')
            origin.sql_query = 'SELECT * FROM {0}'.format(table_name)
            origin.incremental_mode = False
            origin.on_unknown_type = 'CONVERT_TO_STRING'

        trash = builder.add_stage('Trash')

        origin >> trash

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert len(snapshot[origin].output) == 2
        record = snapshot[origin].output[0]
        null_record = snapshot[origin].output[1]

        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.
        # TLKT-177: Add ability for field to return raw value

        assert record.field['data_column'].type == expected_type
        assert null_record.field['data_column'].type == expected_type

        assert record.field['data_column']._data['value'] == expected_value
        assert null_record.field['data_column'] == None
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.0.0.0')
@database('sqlserver')
# https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-2017
# hiearchyid types not supported
# Geometry and geography not supported
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', [
    ('DATE', "'2019-01-01'", 'DATE', 1546300800000),
    ('DATETIME', "'2004-05-23T14:25:10'", 'DATETIME', 1085322310000),
    ('DATETIME2', "'2004-05-23T14:25:10'", 'DATETIME', 1085322310000),
    ('DATETIMEOFFSET', "'2004-05-23 14:25:10.3456 -08:00'", 'DEPENDS_ON_VERSION', 'depends_on_version'),
    ('SMALLDATETIME', "'2004-05-23T14:25:10'", 'DATETIME', 1085322300000),
    ('TIME', "'14:25:10'", 'TIME', 51910000),
    ('BIT', "1", 'BOOLEAN', True),
    ('DECIMAL(5,2)', '5.20', 'DECIMAL', '5.20'),
    ('NUMERIC(5,2)', '5.20', 'DECIMAL', '5.20'),
    ('REAL', '5.20', 'FLOAT', '5.2'),
    ('FLOAT', '5.20', 'DOUBLE', '5.2'),
    ('TINYINT', '255', 'SHORT', 255),
    ('SMALLINT', '-32768', 'SHORT', -32768),
    ('INT', '-2147483648', 'INTEGER', '-2147483648'),
    ('BIGINT', '-9223372036854775807', 'LONG', '-9223372036854775807'),
    ('MONEY', '255.60', 'DECIMAL', '255.6000'),
    ('SMALLMONEY', '255.60', 'DECIMAL', '255.6000'),
    ('BINARY(5)', "CAST('Hello' AS BINARY(5))", 'BYTE_ARRAY', 'SGVsbG8='),
    ('VARBINARY(5)', "CAST('Hello' AS VARBINARY(5))", 'BYTE_ARRAY', 'SGVsbG8='),
    ('CHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('VARCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('NCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('NVARCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('TEXT', "'Hello'", 'STRING', 'Hello'),
    ('NTEXT', "'Hello'", 'STRING', 'Hello'),
    ('IMAGE', "CAST('Hello' AS IMAGE)", 'BYTE_ARRAY', 'SGVsbG8='),
    #    ('GEOGRAPHY',"geography::STGeomFromText('LINESTRING(-122.360 47.656, -122.343 47.656 )', 4326)", 'BYTE_ARRAY', '5hAAAAEUhxbZzvfTR0DXo3A9CpdewIcW2c7300dAy6FFtvOVXsA='),
    #    ('GEOMETRY',"geometry::STGeomFromText('LINESTRING (100 100, 20 180, 180 180)', 0)", 'BYTE_ARRAY', 'AAAAAAEEAwAAAAAAAAAAAFlAAAAAAAAAWUAAAAAAAAA0QAAAAAAAgGZAAAAAAACAZkAAAAAAAIBmQAEAAAABAAAAAAEAAAD/////AAAAAAI='),
    ('XML', "'<a></a>'", 'STRING', '<a/>')
])
@pytest.mark.parametrize('use_table_origin', [True, False])
def test_jdbc_sqlserver_types(sdc_builder, sdc_executor, database, use_table_origin, sql_type, insert_fragment,
                              expected_type, expected_value):
    """Test all feasible SQL Server types."""
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    try:
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

        builder = sdc_builder.get_pipeline_builder()

        if use_table_origin:
            origin = builder.add_stage('JDBC Multitable Consumer')
            origin.table_configs = [{"tablePattern": f'%{table_name}%'}]
        else:
            origin = builder.add_stage('JDBC Query Consumer')
            origin.sql_query = 'SELECT * FROM {0}'.format(table_name)
            origin.incremental_mode = False

        trash = builder.add_stage('Trash')

        # As a part of SDC-10125, DATETIMEOFFSET is natively supported in SDC, and is converted into ZONED_DATETIME
        if sql_type == 'DATETIMEOFFSET':
            if Version(sdc_builder.version) >= Version('3.14.0'):
                expected_type = 'ZONED_DATETIME'
                expected_value = '2004-05-23T14:25:10.3456-08:00'
            else:
                expected_type = 'STRING'
                expected_value = '2004-05-23 14:25:10.3456 -08:00'
                # This unknown_type_action setting is required, otherwise DATETIMEOFFSET tests for SDC < 3.14 will fail.
                origin.on_unknown_type = 'CONVERT_TO_STRING'

        origin >> trash

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert len(snapshot[origin].output) == 2
        record = snapshot[origin].output[0]
        null_record = snapshot[origin].output[1]

        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.
        # TLKT-177: Add ability for field to return raw value

        assert record.field['data_column'].type == expected_type
        assert null_record.field['data_column'].type == expected_type

        assert record.field['data_column']._data['value'] == expected_value
        assert null_record.field['data_column'] == None
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.12.0')
@database('sqlserver')
@pytest.mark.parametrize('on_unknown_type_action', ['CONVERT_TO_STRING', 'STOP_PIPELINE'])
def test_jdbc_sqlserver_on_unknown_type_action(sdc_builder, sdc_executor, database, on_unknown_type_action):
    """Test JDBC Multitable Consumer with MS-SQL server for the on_unknown_type action.
        This is to verify SDC-12764.
        When the 'On Unknown Type' action is set to STOP_PIPELINE,the pipeline should stop with a StageException Error since it cannot convert DATETIMEOFFSET field
        When the 'On Unknown Type' action is set to CONVERT_TO_STRING, the pipeline should convert the unknown type to string and process next record
        The pipeline will look like:
            JDBC_Multitable_Consumer >> trash
    """

    if Version(sdc_builder.version) >= Version('3.14.0'):
        pytest.skip("Skipping SQLServer Unknown Type action check, since DATETIMEOFFSET field is now natively supported from SDC Version 3.14.0")

    column_type = 'DATETIMEOFFSET'
    INPUT_DATE = "'2004-05-23T14:25:10'"
    EXPECTED_OUTCOME = OrderedDict(id=1, date_offset='2004-05-23 14:25:10 +00:00')

    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Setup Origin with specified unknown type action
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[{"tablePattern": f'%{table_name}%'}],
                                            on_unknown_type=on_unknown_type_action)

    # Setup destination
    trash=pipeline_builder.add_stage('Trash')

    # Connect the pipeline stages
    jdbc_multitable_consumer >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    # Create table and add a row
    connection.execute(f"""
        CREATE TABLE {table_name}(
            id int primary key,
            date_offset {column_type} NOT NULL
        )
    """)
    connection.execute(f"INSERT INTO {table_name} VALUES(1, {INPUT_DATE})")

    try:
        if on_unknown_type_action == 'STOP_PIPELINE':
            # Pipeline should stop with StageException
            with pytest.raises(Exception):
                sdc_executor.start_pipeline(pipeline)
                sdc_executor.stop_pipeline(pipeline)

            status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
            assert 'RUN_ERROR' == status
        else:
            snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
            output_records = snapshot[jdbc_multitable_consumer].output

            assert len(output_records) == 1
            assert output_records[0].field == EXPECTED_OUTCOME

    finally:
        status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        if status == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)

        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.14.0')
@database('sqlserver')
def test_jdbc_sqlserver_datetimeoffset_as_primary_key(sdc_builder, sdc_executor, database):
    """Test JDBC Multitable Consumer with SQLServer table configured with DATETIMEOFFSET column as primary key.
        The pipeline will look like:
            JDBC_Multitable_Consumer >> trash
    """
    INPUT_COLUMN_TYPE, INPUT_DATE = 'DATETIMEOFFSET', "'2004-05-23 14:25:10.3456 -08:00'"
    EXPECTED_TYPE, EXPECTED_VALUE = 'ZONED_DATETIME', '2004-05-23T14:25:10.3456-08:00'

    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[{"tablePattern": f'%{table_name}%'}])

    trash=pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    connection.execute(f"""
        CREATE TABLE {table_name}(
            dto {INPUT_COLUMN_TYPE} NOT NULL PRIMARY KEY
        )
    """)
    connection.execute(f"INSERT INTO {table_name} VALUES({INPUT_DATE})")

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert len(snapshot[jdbc_multitable_consumer].output) == 1
        record = snapshot[jdbc_multitable_consumer].output[0]

        assert record.field['dto'].type == EXPECTED_TYPE
        assert record.field['dto'].value == EXPECTED_VALUE

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


# Test for SDC-13288
@database('db2')
def test_jdbc_producer_db2_long_record(sdc_builder, sdc_executor, database):
    """Test that JDBC Producer correctly sends record when setting Custom Data SQLSTATE for db2 database instead of
     throwing StageException. The pipelines reads a file with 5 records 1 by 1 having the last record being biggest
     than the db2 table column size. That throws an error with an specific SQL Code (22001). Having that code in Custom
     Data SQLSTATE sends the last record to error.

     The pipeline looks like:

     directory_origin >> jdbc_producer

     In order to create the file read by directory origin another pipeline is used that looks like:

     dev_raw_data_source >> local_fs
    """

    # Insert data into file.
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    csv_records = ['1,hello', '2,hello', '3,hello', '4,hello', '5,hellolargerword']
    _setup_delimited_file(sdc_executor, tmp_directory, csv_records)

    # Create directory origin.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED',
                             file_name_pattern='sdc*', file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE', files_directory=tmp_directory,
                             batch_size_in_recs=1)

    # Create jdbc producer destination.
    # Create table. db2 internal sets table name in uppercase. Thus using directly ascii uppercase.
    table_name = get_random_string(string.ascii_uppercase, 20)
    database.engine.execute(f'CREATE TABLE {table_name} (id VARCHAR(20) NOT NULL PRIMARY KEY, a VARCHAR(10));')
    field_to_column_mapping = [dict(columnName='ID',
                                    dataType='USE_COLUMN_TYPE',
                                    field='/0',
                                    paramValue='?'),
                               dict(columnName='A',
                                    dataType='USE_COLUMN_TYPE',
                                    field='/1',
                                    paramValue='?')]
    jdbc_producer = pipeline_builder.add_stage('JDBC Producer')
    jdbc_producer.set_attributes(default_operation="INSERT",
                                 schema_name=DEFAULT_DB2_SCHEMA,
                                 table_name=table_name,
                                 field_to_column_mapping=field_to_column_mapping,
                                 stage_on_record_error='TO_ERROR',
                                 data_sqlstate_codes=["22001"])

    directory >> jdbc_producer

    directory_jdbc_producer_pipeline = pipeline_builder.build(
        title='Directory - JDBC Producer. Test DB2 sql code error').configure_for_environment(database)
    sdc_executor.add_pipeline(directory_jdbc_producer_pipeline)

    try:
        snapshot = sdc_executor.capture_snapshot(directory_jdbc_producer_pipeline, start_pipeline=True, batch_size=1,
                                                 batches=5).snapshot
        sdc_executor.stop_pipeline(directory_jdbc_producer_pipeline)

        assert 5 == len(snapshot.snapshot_batches)

        result = database.engine.execute(f'SELECT ID,A FROM {table_name};')
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # Order by id.
        result.close()

        # Assert records in database include from id=1 to id=4 excluding id=5. Columns => record[0] = id, record[1] = a.
        assert data_from_database == [(record[0], record[1]) for record in
                                      [unified_record.split(',') for unified_record in csv_records[:-1]]]

        stage = snapshot.snapshot_batches[4][jdbc_producer.instance_name]

        assert 1 == len(stage.error_records)

        error_record = stage.error_records[0]

        assert 'hellolargerword' == error_record.field['1']
        assert 'JDBC_14' == error_record.header['errorCode']
        assert 'SQLSTATE=22001' in error_record.header['errorMessage']

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        database.engine.execute(f'DROP TABLE {table_name}')


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
@sdc_min_version('3.15.0')
def test_multitable_quote_column_names(sdc_builder, sdc_executor, database):
    """
    Ensure that we properly quote all table and column names when querying the database.
    """
    table_name = "table_" + get_random_string(string.ascii_letters, 10)
    offset_name = "column_" + get_random_string(string.ascii_letters, 10)

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('JDBC Multitable Consumer')
    origin.table_configs=[{"tablePattern": f'%{table_name}%'}]
    origin.max_batch_size_in_records = 10

    trash = builder.add_stage('Trash')

    origin >> trash

    pipeline = builder.build().configure_for_environment(database)
    # Work-arounding STF behavior of upper-casing table name configuration
    origin.table_configs[0]["tablePattern"] = f'%{table_name}%'

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column(offset_name, sqlalchemy.Integer, primary_key=True, quote=True),
        quote = True
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), [{offset_name: 1}])

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        # We want to run for a few seconds to see if any errors show up (like that did in previous versions)
        time.sleep(10)
        sdc_executor.stop_pipeline(pipeline)

        # There should be no errors reported
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('stage.JDBCMultitableConsumer_01.errorRecords.counter').count == 0
        assert history.latest.metrics.counter('stage.JDBCMultitableConsumer_01.stageErrors.counter').count == 0

        # And verify that we properly read that one record
        assert len(snapshot[origin].output) == 1
        assert snapshot[origin].output[0].get_field_data('/' + offset_name) == 1
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)

@database
@sdc_min_version('3.0.0.0')
def test_jdbc_multitable_consumer_duplicates_read_when_initial_offset_configured(sdc_builder, sdc_executor, database):
    """
    SDC-13625 Integration test for SDC-13624 - MT Consumer ingests duplicates when initial offset is specified
    Setup origin as follows:
        partitioning enabled + num_threads and num partitions > 1 + override offset column set
        + initial value specified for offset

    Verify that origin does not ingest the records more than once (duplicates) when initial value for offset is set

    Pipeline:
        JDBC MT Consumer >> Trash
                         >= Pipeline Finisher (no-more-data)
    """
    if database.type == 'Oracle':
        pytest.skip("This test depends on proper case for column names that Oracle auto-uppers.")

    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, 20))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[{
        "tablePattern": f'{table_name}',
        "enableNonIncremental": False,
        "partitioningMode": "REQUIRED",
        "partitionSize": "100000",
        "maxNumActivePartitions": 5,
        'overrideDefaultOffsetColumns': True,
        'offsetColumns': ['created'],
        'offsetColumnToInitialOffsetValue': [{
            'key': 'created',
            'value': '0'
        }]
    }])

    jdbc_multitable_consumer.number_of_threads = 2
    jdbc_multitable_consumer.maximum_pool_size = 2

    trash = pipeline_builder.add_stage('Trash')
    jdbc_multitable_consumer >> trash

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    finisher.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']
    jdbc_multitable_consumer >= finisher

    pipeline = pipeline_builder.build().configure_for_environment(database)
    ONE_MILLION = 1000000
    rows_in_table = [{'id': i, 'name': get_random_string(string.ascii_lowercase, 5), 'created': i + ONE_MILLION}
                     for i in range(1, 21)]

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(5)),
        sqlalchemy.Column('created', sqlalchemy.Integer)
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding 20 rows into %s table', table_name)
        connection = database.engine.connect()

        connection.execute(table.insert(), rows_in_table)
        connection.close()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, batches=2, start_pipeline=True).snapshot

        rows_from_snapshot = [(record.get_field_data('/name').value,
                               record.get_field_data('/id').value,
                               record.get_field_data('/created').value)
                              for batch in snapshot.snapshot_batches
                              for record in batch.stage_outputs[jdbc_multitable_consumer.instance_name].output]

        expected_data = [(row['name'], row['id'], row['created']) for row in rows_in_table]
        assert rows_from_snapshot == expected_data
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)
