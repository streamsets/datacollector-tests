# Copyright 2022 StreamSets Inc.
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
import string

import pytest
import sqlalchemy
from streamsets.testframework.utils import get_random_string, Version

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.sdc_min_version('5.4.0'), pytest.mark.database('postgresql'), pytest.mark.skip]

# from CommonDatabaseHeader.java
PRIMARY_KEY_COLUMN_OLD_VALUE = 'jdbc.primaryKey.before'
PRIMARY_KEY_COLUMN_NEW_VALUE = 'jdbc.primaryKey.after'

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Ghastly'},
    {'id': 2, 'name': 'Haunter'},
    {'id': 3, 'name': 'Gengar'}
]
ROWS_TO_UPDATE = [
    {'id': 2, 'name': 'Kyogre'},
    {'id': 4, 'name': 'Groudon'}
]
RAW_DATA = ['name'] + [row['name'] for row in ROWS_IN_DATABASE]

CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY = [
    {'TYPE': 'Hobbit', 'ID': 1, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 0'},
    {'TYPE': 'Fallohide', 'ID': 1, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 1'},
    {'TYPE': 'Fallohide', 'ID': 2, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 2'},
    {'TYPE': 'Hobbit - Fallohide', 'ID': 3, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 3'},
    {'TYPE': 'Hobbit, Fallohide', 'ID': 3, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 4'},
    {'TYPE': 'Hobbit, Fallohide', 'ID': 4, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 5'},
    {'TYPE': 'Hobbit, Fallohide', 'ID': 4, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 6'}
]
CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY_EXPECTED_DATA = [
    ('Hobbit, Fallohide', 4, 'Bilbo', 'Baggins', 'Bag End 6')
]
CDC_PK_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY_HEADER = [
    {'sdc.operation.type': 1, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 1, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 1,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 1, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 1,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Fallohide'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 1, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 2,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Fallohide', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Fallohide'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 2, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 3,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Fallohide', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit - Fallohide'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 3, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 3,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit - Fallohide', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit, Fallohide'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 3, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 4,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit, Fallohide', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit, Fallohide'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 4, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 4,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit, Fallohide', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit, Fallohide'},
]


def _get_random_name(prefix=''):
    """Generate a random lowercase string to use as a database object name.

    Args:
        database: a :obj:`streamsets.testframework.environment.Database` object.
        prefix: (:obj:`str`) add a prefix to the generated name. Default: ''.
        length: (:obj:`int`) number of characters of the generated name (without counting ``prefix``).

    """
    name = '{}{}'.format(prefix.lower(), get_random_string(string.ascii_lowercase))

    return name


def _get_random_schema_name(database, prefix='', length=5):
    """Generate a lowercase random string to use as a database schema name.

    Args:
        database: a :obj:`streamsets.testframework.environment.Database` object.
        prefix: (:obj:`str`) add a prefix to the generated name. Default: ''.
        length: (:obj:`int`) number of characters of the generated name (without counting ``prefix``).

    """
    return _get_random_name(prefix=prefix)


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

    Use ``_drop_schema()`` to remove schemas created by this function, to handle properly each case.

    Args:
        schema_name: (:obj:`str`) the schema name.
        database: a :obj:`streamsets.testframework.environment.Database` object.

    """
    schema = sqlalchemy.schema.CreateSchema(schema_name)
    database.engine.execute(schema)


def _drop_schema(schema_name):
    """Remove a schema from the given database.

    Args:
        schema_name: (:obj:`str`) name of the schema to remove.
        database: a :obj:`streamsets.testframework.environment.Database` object.

    """
    sqlalchemy.schema.DropSchema(schema_name)


def _create_postgres_producer_pipeline(pipeline_builder, pipeline_title, raw_data, table_name, operation):
    """Helper function to create and return a pipeline with PostgreSQL Producer
    The Deduplicator assures there is only one ingest to database. The pipeline looks like:
    The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> postgres_producer
                               record_deduplicator >> trash
    """
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')

    FIELD_MAPPINGS = [dict(field='/id', columnName='id'),
                      dict(field='/name', columnName='name')]
    postgres_producer = pipeline_builder.add_stage('PostgreSQL Producer')
    postgres_producer.set_attributes(default_operation=operation,
                                 table_name=table_name,
                                 field_to_column_mapping=FIELD_MAPPINGS,
                                 stage_on_record_error='STOP_PIPELINE',
                                 ssl_mode='DISABLED')

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> record_deduplicator >> postgres_producer
    record_deduplicator >> trash

    return pipeline_builder.build(title=pipeline_title)


def test_postgres_producer_insert_type_err(sdc_builder, sdc_executor, database):
    """This test covers invalid type coersion - writing string into int column. This test verifies that we can properly
    catch and generate JDBC_23 in PostgreSQL.
    """

    ROWS_IN_DATABASE = [
        {'id': 1, 'name': 'Ghastly'},
        {'id': 'X', 'name': 'Haunter'},
        {'id': 3, 'name': 'Gengar'}
    ]

    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)

    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=DATA, stop_after_first_batch=True)

    FIELD_MAPPINGS = [dict(field='/id', columnName='id', dataType='INTEGER'),
                      dict(field='/name', columnName='name', dataType='STRING')]
    postgres_producer = pipeline_builder.add_stage('PostgreSQL Producer')
    postgres_producer.set_attributes(default_operation='INSERT',
                                 table_name=table_name,
                                 field_to_column_mapping=FIELD_MAPPINGS,
                                 stage_on_record_error='TO_ERROR',
                                 ssl_mode='DISABLED')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> [postgres_producer, wiretap.destination]

    pipeline = pipeline_builder.build(title="PostgreSQL producer with error")

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


# SDC-15039: Data loss when PostgreSQL Producer doesn't match any columns
@pytest.mark.parametrize('multi_row', [True, False])
@pytest.mark.parametrize('field_mapping', [True, False])
def test_postgres_producer_no_implicit_mapping(sdc_builder, sdc_executor, database, multi_row, field_mapping):
    """This test covers situation when neither of the record fields matches the destination table - in such cases
    the record should ended up in error stream.
    """

    # Every second record have columns not available in the target table and should end up inside error stream
    INSERT_DATA = [
        {'id': 1, 'name': 'Ghastly'},
        {'date_of_birth': 'yesterday'},
        {'id': 2, 'name': 'Haunter'},
        {'date_of_birth': 'tomorrow'},
        # These extra rows will actually hit the multi-row block of code causing it to fail on db's that don't support
        # multi-row
        {'id': 3, 'name': 'Haunter1'},
        {'id': 4, 'name': 'Haunter2'},
        {'id': 5, 'name': 'Haunter3'},
        {'id': 6, 'name': 'Haunter4'},

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
    postgres_producer = pipeline_builder.add_stage('PostgreSQL Producer')
    postgres_producer.set_attributes(default_operation='INSERT',
                                 table_name=table_name,
                                 use_multi_row_operation=multi_row,
                                 field_to_column_mapping=FIELD_MAPPINGS,
                                 stage_on_record_error='TO_ERROR',
                                 ssl_mode='DISABLED')
    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> [postgres_producer, wiretap.destination]

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


def test_postgres_producer_coerced_insert(sdc_builder, sdc_executor, database):
    """Extension of the Simple PostgreSQL Producer test with INSERT operation.
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
        {'id': '1', 'name': 'Ghastly'},
        {'id': '2', 'name': 'Haunter'},
        {'id': '3', 'name': 'Gengar'}
    ]

    DATA = '\n'.join(json.dumps(rec) for rec in COERCE_ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    pipeline = _create_postgres_producer_pipeline(pipeline_builder, 'PostgreSQL Producer Insert', DATA, table_name, 'INSERT')
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


def test_postgres_producer_delete(sdc_builder, sdc_executor, database):
    """Simple PostgreSQL Producer test with DELETE operation.
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

    pipeline = _create_postgres_producer_pipeline(pipeline_builder, 'PostgreSQL Producer Delete', DATA, table_name, 'DELETE')
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


def test_postgres_producer_update(sdc_builder, sdc_executor, database):
    """Simple PostgreSQL Producer test with UPDATE operation.
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

    pipeline = _create_postgres_producer_pipeline(pipeline_builder, 'PostgreSQL Producer Update', DATA, table_name, 'UPDATE')
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
def test_postgres_producer_multirow_with_duplicates(sdc_builder, sdc_executor, database):
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

    producer = builder.add_stage('PostgreSQL Producer')
    producer.table_name = table_name
    producer.field_to_column_mapping = []
    producer.default_operation = 'INSERT'
    producer.use_multi_row_operation = True
    producer.ssl_mode = 'DISABLED'

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


def test_postgres_producer_multitable(sdc_builder, sdc_executor, database):
    """Test for PostgreSQL Producer with multiple destination table. We create 3 tables in the default schema and use an EL
    expression to insert records according to the /table record field.

    Pipeline:
        dev_raw_data_source >> record_deduplicator >> postgres_producer
                               record_deduplicator >> trash

    """
    table1_name = _get_random_name(prefix='stf_table_')
    table2_name = _get_random_name(prefix='stf_table_')
    table3_name = _get_random_name(prefix='stf_table_')

    table1 = _create_table(table1_name, database)
    table2 = _create_table(table2_name, database)
    table3 = _create_table(table3_name, database)

    ROWS = [{'table': table1_name, 'id': 1, 'name': 'Roger Federer'},
            {'table': table2_name, 'id': 2, 'name': 'Rafael Nadal'},
            {'table': table3_name, 'id': 3, 'name': 'Dominic Thiem'}]

    INPUT_DATA = '\n'.join(json.dumps(rec) for rec in ROWS)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline = _create_postgres_producer_pipeline(pipeline_builder, 'PostgreSQL Producer Multitable Insert', INPUT_DATA,
                                              "${record:value('/table')}", 'INSERT')

    # EL function names are case-sensitive; we overwrite it afterwards to avoid an ELerror.
    pipeline.configure_for_environment(database)
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
        logger.info('Dropping tables %s, %s, %s in %s database...', table1_name, table2_name, table3_name,
                    database.type)
        table1.drop(database.engine)
        table2.drop(database.engine)
        table3.drop(database.engine)


# Test SDC-10719
def test_postgres_producer_multischema(sdc_builder, sdc_executor, database):
    """Test for PostgreSQL Producer in a multischema scenario with a single destination table for each schema. We create 3
    schemas with one table for each, with the same name. Then we use an EL expression to insert records according to
    the /schema record field.

    Pipeline:
        dev_raw_data_source >> record_deduplicator >> postgres_producer
                               record_deduplicator >> trash

    """
    schema1_name = _get_random_schema_name(database, prefix='stf_schema_')
    schema2_name = _get_random_schema_name(database, prefix='stf_schema_')
    schema3_name = _get_random_schema_name(database, prefix='stf_schema_')
    table_name = _get_random_name(prefix='stf_table_')

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
    pipeline = _create_postgres_producer_pipeline(pipeline_builder, 'PostgreSQL Producer Multischema Insert', INPUT_DATA,
                                              table_name, 'INSERT')

    # PostgreSQL Producer's "Schema Name" property is set through the `database` environment under some circumstances
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
        _drop_schema(schema1_name)
        _drop_schema(schema2_name)
        _drop_schema(schema3_name)


# Test SDC-10719
def test_postgres_producer_multischema_multitable(sdc_builder, sdc_executor, database):
    """Test a PostgreSQL Producer in a multischema scenario with different destination tables for each schema. We create 3
    schemas with one table for each, with different names. Then we use an EL expressions to insert records according to
    the /schema and /table record fields.

    Pipeline:
        dev_raw_data_source >> record_deduplicator >> postgres_producer
                               record_deduplicator >> trash

    """
    schema1_name = _get_random_schema_name(database, prefix='stf_schema_')
    schema2_name = _get_random_schema_name(database, prefix='stf_schema_')
    schema3_name = _get_random_schema_name(database, prefix='stf_schema_')
    table1_name = _get_random_name(prefix='stf_table_')
    table2_name = _get_random_name(prefix='stf_table_')
    table3_name = _get_random_name(prefix='stf_table_')

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
    pipeline = _create_postgres_producer_pipeline(pipeline_builder, 'PostgreSQL Producer Multischema and Multitable Insert',
                                              INPUT_DATA, "${record:value('/table')}", 'INSERT')

    # PostgreSQL Producer's "Schema Name" property is set through the `database` environment under some circumstances
    # (e.g. Sql Server database). We overwrite it afterwards for the test.
    pipeline.configure_for_environment(database)
    pipeline[2].set_attributes(schema_name="${record:value('/schema')}")

    # EL function names are case-sensitive; we overwrite it afterwards to avoid an EL error.
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
        _drop_schema(schema1_name)
        _drop_schema(schema2_name)
        _drop_schema(schema3_name)


# SDC-11063: Do not reoder update statements in PostgreSQL destination
@pytest.mark.parametrize('multi_row', [True, False])
def test_postgres_producer_ordering(sdc_builder, sdc_executor, multi_row, database):
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

    producer = builder.add_stage('PostgreSQL Producer')
    producer.field_to_column_mapping = []
    producer.default_operation = 'UPDATE'
    producer.table_name = table_name
    producer.use_multi_row_operation = multi_row
    producer.ssl_mode = 'DISABLED'

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


@pytest.mark.parametrize('dyn_table', [False, True])
@pytest.mark.parametrize('multi_row', [False, True])
def test_error_handling_when_there_is_no_primary_key(sdc_builder, sdc_executor, database, dyn_table, multi_row):
    """
    SDC-12960. Updating PostgreSQL table with no PK results in NPE

    Tests if a user friendly error will be added to record errors, one error for each failing record,
    instead of breaking the whole pipeline when UPDATE or DELETE operation is used and a destination table has no
    primary key.
    Covers cases with/without dynamic tables, with/without multi row parameter enabled.

    The pipeline:
    Dev Raw Data Source->Expression Evaluator->Field Remover->PostgreSQL Producer

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
    table_name_expression = ("${record:attribute('tbl')}") if dyn_table else table_names[0]
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

    producer = builder.add_stage('PostgreSQL Producer')
    producer.field_to_column_mapping = []
    producer.default_operation = 'UPDATE'
    producer.table_name = table_name_expression
    producer.use_multi_row_operation = multi_row
    producer.ssl_mode = 'DISABLED'

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


def test_postgres_producer_postgres_partitioned(sdc_builder, sdc_executor, database):
    """
    Make sure that PostgreSQL treats partitioned tables as expected.
    """
    postgres_version = database.engine.execute("SELECT current_setting('server_version');").fetchall()[0][0]
    if Version(postgres_version) < Version(10):
        pytest.skip('Partition test only supported for PostgreSQL with version greater than 10')

    table_name = get_random_string(string.ascii_lowercase, 15)
    part_table_name = get_random_string(string.ascii_lowercase, 15)

    builder = sdc_builder.get_pipeline_builder()

    # Generate batch that will repeat the same primary key in the middle of the batch (on third row)
    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'JSON'
    source.raw_data = """{"id" : 1}\n{"id" : 2}\n{"id" : 3}\n{"id" : 4}"""

    producer = builder.add_stage('PostgreSQL Producer')
    producer.table_name = table_name
    producer.field_to_column_mapping = []
    producer.default_operation = 'INSERT'
    producer.use_multi_row_operation = True
    producer.statement_parameter_limit = 4
    producer.ssl_mode = 'DISABLED'

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


@pytest.mark.parametrize('rollback_enabled', [True, False])
def test_postgres_producer_multirow_with_duplicates_error(sdc_builder, sdc_executor, database, rollback_enabled):
    """
    Make sure that when using Multi Row insert, data related errors are send to error stream and the rest of the batch
    is processed properly.
    """

    table_name = get_random_string(string.ascii_lowercase, 15)

    builder = sdc_builder.get_pipeline_builder()

    # Generate batch that will repeat the same primary key in the middle of the batch (on third row)
    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'JSON'
    source.raw_data = """{"id" : 1, "u": 1}\n{"id" : 2, "u": 2}\n{"id" : 3, "u": 3}\n{"id" : 4, "u": 4}\n
    {"id" : 5, "u": 5}\n{"id" : 6, "u": 6}\n{"id" : 7, "u": 1}\n{"id" : 8, "u": 8}"""

    producer = builder.add_stage('PostgreSQL Producer')
    producer.table_name = table_name
    producer.field_to_column_mapping = []
    producer.default_operation = 'INSERT'
    producer.use_multi_row_operation = True
    producer.statement_parameter_limit = 4
    producer.rollback_batch_on_error = rollback_enabled
    producer.ssl_mode = 'DISABLED'

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
        assert len(data_from_database) == 0
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@pytest.mark.parametrize('multi_row', [False, True])
def test_postgres_producer_primary_key_header_update(sdc_builder, sdc_executor, database, multi_row):
    """
    Test to make sure Primary Key updates are handled correctly. We will insert one record, and then
    perform multiple updates to its columns (including Primary Key columns) and check we end up
    having just one row in the table, with the expected values.
    The pipeline looks like:

        dev_raw_data_source >> expression_evaluator >> field_remover >> postgres_producer

    """

    table_name = get_random_string(string.ascii_lowercase, 15)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw Data Source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    rows = copy.deepcopy(CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY)
    for row, header in zip(rows, CDC_PK_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY_HEADER):
        row['HEADER'] = header
    raw_data = '\n'.join((json.dumps(row) for row in rows))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    # We assume CDC headers are generated properly
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "${record:value('/HEADER/sdc.operation.type')}"},
        {'attributeToSet': f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID',
         'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_OLD_VALUE + ".ID')}"},
        {'attributeToSet': f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID',
         'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_NEW_VALUE + ".ID')}"},
        {'attributeToSet': f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE',
         'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_OLD_VALUE + ".TYPE')}"},
        {'attributeToSet': f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE',
         'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_NEW_VALUE + ".TYPE')}"}
    ])

    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/HEADER']

    # Jdbc Producer
    postgres_producer = pipeline_builder.add_stage('PostgreSQL Producer')
    postgres_producer.set_attributes(default_operation='INSERT',
                                 table_name=table_name,
                                 field_to_column_mapping=[],
                                 use_multi_row_operation=multi_row,
                                 stage_on_record_error='TO_ERROR',
                                 enclose_object_names=True,
                                 ssl_mode='DISABLED')

    dev_raw_data_source >> expression_evaluator >> field_remover >> postgres_producer

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                             sqlalchemy.Column('TYPE', sqlalchemy.String(64), primary_key=True),
                             sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
                             sqlalchemy.Column('NAME', sqlalchemy.String(64)),
                             sqlalchemy.Column('SURNAME', sqlalchemy.String(64)),
                             sqlalchemy.Column('ADDRESS', sqlalchemy.String(64)))
    try:
        logger.info('Creating source table %s in %s database ...', table_name, database.type)
        table.create(database.engine)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        result = database.engine.execute(table.select())
        data_from_database = result.fetchall()
        result.close()

        # We assert that the pipeline processed 1 insert and 6 updates
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 7
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 0
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 7

        # And assert that in the database we end up having just 1 row, with the latest update value
        assert len(data_from_database) == 1
        assert data_from_database == CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY_EXPECTED_DATA
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)
