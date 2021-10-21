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
import math
import string

import pytest
import sqlalchemy
from streamsets.testframework.environments.databases import OracleDatabase, SQLServerDatabase, PostgreSqlDatabase
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
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> [jdbc_producer, wiretap.destination]

    pipeline = pipeline_builder.build(title="JDBC producer with error")

    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()

        assert data_from_database == [(record['name'], record['id']) for record in ROWS_IN_DATABASE
                                      if record['id'] != 'X']

        assert 'JDBC_23' == wiretap.error_records[0].header['errorCode']

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

    if isinstance(database, OracleDatabase) and multi_row:
        pytest.skip('multi_row is not supported on oracle databases')

    # Every second record have columns not available in the target table and should end up inside error stream
    INSERT_DATA = [
        {'id': 1, 'name': 'Dima'},
        {'date_of_birth': 'yesterday'},
        {'id': 2, 'name': 'Arvind'},
        {'date_of_birth': 'tomorrow'},
        # These extra rows will actually hit the multi-row block of code causing it to fail on db's that don't support
        # multi-row
        {'id': 3, 'name': 'Arvind1'},
        {'id': 4, 'name': 'Arvind2'},
        {'id': 5, 'name': 'Arvind3'},
        {'id': 6, 'name': 'Arvind4'},

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
    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> [jdbc_producer, wiretap.destination]

    pipeline = pipeline_builder.build().configure_for_environment(database)

    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.error_records) == 2
        assert 'JDBC_90' == wiretap.error_records[0].header['errorCode']
        assert 'JDBC_90' == wiretap.error_records[1].header['errorCode']

        result = database.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()

        assert data_from_database == [(record['name'], record['id']) for record in INSERT_DATA if 'id' in record]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


# SDC-10786: This test intends to cover the case really precise decimals being inserted into a Float column in MSSQL
@database('sqlserver')
def test_mssql_producer_bigdecimal(sdc_builder, sdc_executor, database):
    """
    Insert a Decimal value with up to 38 decimals into a Float column in MSSQL.
    This will look like:
    dev_data_generator >> [jdbc_producer, wiretap]
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
    wiretap = pipeline_builder.add_wiretap()
    dev_data_generator >> [jdbc_producer, wiretap.destination]

    pipeline = pipeline_builder.build('MSSQL BigDecimal')
    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)

        sdc_executor.stop_pipeline(pipeline)

        records = [record.field for record in wiretap.output_records]

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
        pytest.skip(
            'This test is trying to insert explicit value to identity column which is not supported on SQL Server')

    if isinstance(database, OracleDatabase):
        pytest.skip('multi_row is not supported on oracle databases')

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

    if isinstance(database, OracleDatabase) and multi_row:
        pytest.skip('multi_row is not supported on oracle databases')

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
@database('oracle')
def test_jdbc_producer_oracle_data_errors(sdc_builder, sdc_executor, database):
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
    producer.use_multi_row_operation = False

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

    if isinstance(database, OracleDatabase) and multi_row:
        pytest.skip('multi_row is not supported on oracle databases')

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

    wiretap = builder.add_wiretap()
    data_source >> expression >> remover >> [producer, wiretap.destination]

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

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        logger.info('pipeline: %s', pipeline)
        assert expected_error_record_count == len(wiretap.error_records)
        for i in range(0, expected_error_record_count):
            assert 'JDBC_62' == wiretap.error_records[i].header['errorCode']
    finally:
        for i in range(0, created_table_count):  # We will drop only the tables we have created
            tables[i].drop(database.engine)
        connection.close()


# COLLECTOR-88: JDBC Producer truncates decimals when using multi-row operations against SQL Server
@database('sqlserver')
def test_mssql_producer_decimal_precision(sdc_builder, sdc_executor, database):
    """
    JDBC SQLServer Producer truncated some decimals prior to the implementation of COLLECTOR-88. The issue is only
    reproducible when certain conditions are met:
    - The data comes coded with different values of decimal precision.
    - Multi-row operations are enabled.

    To reproduce this scenario, the pipeline is the following:

    jdbc_multitable_consumer >> stream_selector >>                      >> jdbc_producer
                                                   field_type_converter >>

    The stream selector separates initial data in two streams, and one of them is converted from Numeric(5,3) to
    Numeric(5,2) without losing precision since those values already had only 2 decimals. When trying to insert the data
    into SQLServer again, everything should be converted to Numeric(5,3) again so precision is not lost.
    """
    table_name_origin = get_random_string(string.ascii_lowercase, 20)
    table_name_dest = get_random_string(string.ascii_lowercase, 20)

    rows_in_table = [{'data': 11.11},
                     {'data': 22.22},
                     {'data': 33.33},
                     {'data': 55.55555},
                     {'data': 66.66666},
                     {'data': 77.77777}]

    # Create origin table and add some values to it
    table_origin = sqlalchemy.Table(
        table_name_origin,
        sqlalchemy.MetaData(),
        sqlalchemy.Column('data', sqlalchemy.Numeric(7,5), primary_key=True)
    )
    table_origin.create(database.engine)
    connection = database.engine.connect()
    connection.execute(table_origin.insert(), rows_in_table)
    connection.close()

    # Create destination table
    table_dest = sqlalchemy.Table(
        table_name_dest,
        sqlalchemy.MetaData(),
        sqlalchemy.Column('data', sqlalchemy.Numeric(7,5), primary_key=True)
    )
    table_dest.create(database.engine)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[{"tablePattern": f'%{table_name_origin}%'}])

    stream_selector = pipeline_builder.add_stage('Stream Selector')

    field_type_converter_configs = [
        {
            'sourceType': 'DECIMAL',
            'targetType': 'DECIMAL',
            'dataLocale': 'en,US',
            'scale': 2,
            'decimalScaleRoundingStrategy': 'ROUND_UNNECESSARY'
        }
    ]

    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter.set_attributes(conversion_method='BY_TYPE',
                                        whole_type_converter_configs=field_type_converter_configs)

    jdbc_producer = pipeline_builder.add_stage('JDBC Producer')
    jdbc_producer.set_attributes(default_operation='INSERT',
                                 table_name=table_name_dest,
                                 field_to_column_mapping=[],
                                 use_multi_row_operation=True)
    wiretap = pipeline_builder.add_wiretap()

    jdbc_multitable_consumer >> stream_selector
    stream_selector >> [jdbc_producer, wiretap.destination]
    stream_selector >> field_type_converter >> [jdbc_producer, wiretap.destination]

    # Stream Selector conditions depend on the output lanes, so we set them after connecting the stages.
    stream_selector.condition = [{'outputLane': stream_selector.output_lanes[0],
                                  'predicate': '${record:value("/data") > 50}'},
                                 {'outputLane': stream_selector.output_lanes[1],
                                  'predicate': 'default'}]

    pipeline = pipeline_builder.build('MSSQL MultiRow decimals').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(6)
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table_dest.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])  # order by value
        result.close()

        expected_data = [(row['data']) for row in rows_in_table]

        for i in range(0, len(expected_data)):
            assert math.isclose(float(str(data_from_database[i][0])), expected_data[i], rel_tol=0.00002)

    finally:
        logger.info('Dropping tables in %s database ...', database.type)
        table_origin.drop(database.engine)
        table_dest.drop(database.engine)


@sdc_min_version('4.2.0')
@database
def test_jdbc_producer_postgres_partitioned(sdc_builder, sdc_executor, database):
    """
    Make sure that PostgreSQL treats partitioned tables as expected.
    """
    if not isinstance(database, PostgreSqlDatabase):
        pytest.skip('Partition test only supported for PostgreSQL')

    table_name = get_random_string(string.ascii_lowercase, 15)
    part_table_name = get_random_string(string.ascii_lowercase, 15)

    builder = sdc_builder.get_pipeline_builder()

    # Generate batch that will repeat the same primary key in the middle of the batch (on third row)
    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'JSON'
    source.raw_data = """{"id" : 1}\n{"id" : 2}\n{"id" : 3}\n{"id" : 4}"""

    producer = builder.add_stage('JDBC Producer')
    producer.table_name = table_name
    producer.field_to_column_mapping = []
    producer.default_operation = 'INSERT'
    producer.use_multi_row_operation = True
    producer.statement_parameter_limit = 4
    if database.type == 'Oracle':
        producer.enclose_object_names = True

    source >> producer

    pipeline = builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=False),
        postgresql_partition_by='RANGE (id)'
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)
        database.engine.execute(f"CREATE TABLE {part_table_name} PARTITION OF {table_name} FOR VALUES FROM (1) TO (10)")

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 4
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 0
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 4

        # And similarly the database side should be empty as well
        result = database.engine.execute(table.select())
        data_from_database = result.fetchall()
        result.close()
        assert len(data_from_database) == 4
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@sdc_min_version('4.2.0')
@database
@pytest.mark.parametrize('rollback_enabled', [True, False])
def test_jdbc_producer_multirow_with_duplicates_error(sdc_builder, sdc_executor, database, rollback_enabled):
    """
    Make sure that when using Multi Row insert, data related errors are send to error stream and the rest of the batch
    is processed properly.
    """
    if isinstance(database, OracleDatabase):
        pytest.skip('multi_row is not supported on oracle databases')

    table_name = get_random_string(string.ascii_lowercase, 15)

    builder = sdc_builder.get_pipeline_builder()

    # Generate batch that will repeat the same primary key in the middle of the batch (on third row)
    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'JSON'
    source.raw_data = """{"id" : 1, "u": 1}\n{"id" : 2, "u": 2}\n{"id" : 3, "u": 3}\n{"id" : 4, "u": 4}\n
    {"id" : 5, "u": 5}\n{"id" : 6, "u": 6}\n{"id" : 7, "u": 1}\n{"id" : 8, "u": 8}"""

    producer = builder.add_stage('JDBC Producer')
    producer.table_name = table_name
    producer.field_to_column_mapping = []
    producer.default_operation = 'INSERT'
    producer.use_multi_row_operation = True
    producer.statement_parameter_limit = 4
    producer.rollback_batch_on_error = rollback_enabled
    if database.type == 'Oracle':
        producer.enclose_object_names = True

    source >> producer

    pipeline = builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=False),
        sqlalchemy.Column('u', sqlalchemy.Integer, primary_key=True, autoincrement=False)
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Since we are inserting duplicate primary key, the batch should fail
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 8
        # We are inserting one bad record in 8 records, in 4 statements. This means we will have 2 error records,
        # as one statement failed
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 2
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 6

        # And similarly the database side should be empty as well
        result = database.engine.execute(table.select())
        data_from_database = result.fetchall()
        result.close()
        if rollback_enabled or isinstance(database, PostgreSqlDatabase):
            # When rollback enabled, the whole batch should be rolled back
            assert len(data_from_database) == 0
        else:
            assert len(data_from_database) == 6
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)

