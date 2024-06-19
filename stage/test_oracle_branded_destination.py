# Copyright 2024 StreamSets Inc.
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

import pytest
import sqlalchemy
from streamsets.sdk.exceptions import RunError, StartError
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

logger = logging.getLogger(__name__)

ORACLE_DESTINATION = 'com_streamsets_pipeline_stage_destination_OracleDTarget'
pytestmark = [database("oracle"), sdc_min_version('5.10.0')]

ROWS_IN_DATABASE = [
    {'ID': 1, 'NAME': 'Ghastly'},
    {'ID': 2, 'NAME': 'Haunter'},
    {'ID': 3, 'NAME': 'Gengar'}
]
ROWS_TO_UPDATE = [
    {'ID': 2, 'NAME': 'Kyogre'},
    {'ID': 4, 'NAME': 'Groudon'}
]

RAW_DATA = ['NAME'] + [row['NAME'] for row in ROWS_IN_DATABASE]


def _get_random_name(prefix=''):
    """Generate a random string to use as a database object name.

    Args:
        prefix: (:obj:`str`) add a prefix to the generated name. Default: ''.

    """
    name = '{}{}'.format(prefix.upper(), get_random_string(string.ascii_uppercase))

    return name


def _get_random_schema_name(prefix=''):
    """Generate a random string to use as a database schema name.

    Args:
        database: a :obj:`streamsets.testframework.environment.Database` object.
        prefix: (:obj:`str`) add a prefix to the generated name. Default: ''.
        length: (:obj:`int`) number of characters of the generated name (without counting ``prefix``).

    """
    prefix = f'C##{prefix}'
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
                             sqlalchemy.Column('NAME', sqlalchemy.String(32)),
                             sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
                             schema=schema_name)

    logger.info('Creating table %s in %s database ...', table_name, database.type)
    table.create(database.engine)
    return table


def _create_empty_table(table_name, database, schema_name=None):
    """Helper function to create a table with only a primary key column id (int).

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
                             sqlalchemy.Column('NAME', sqlalchemy.String(32)),
                             schema=schema_name)

    logger.info('Creating table %s in %s database ...', table_name, database.type)
    table.create(database.engine)
    return table


def _create_schema(schema_name, database):
    """Create a new schema in the database.

    Creates a new Oracle user.

    Use ``_drop_schema()`` to remove schemas created by this function, to handle properly each case.

    Args:
        schema_name: (:obj:`str`) the schema name.
        database: a :obj:`streamsets.testframework.environment.Database` object.

    """
    database.engine.execute('CREATE USER {user} IDENTIFIED BY {pwd}'.format(user=schema_name, pwd=schema_name))
    database.engine.execute('ALTER USER {user} QUOTA UNLIMITED ON USERS'.format(user=schema_name))
    database.engine.execute('GRANT CONNECT, RESOURCE TO {user}'.format(user=schema_name))


def _drop_schema(schema_name, database):
    """Remove a schema from the given database.

    Args:
        schema_name: (:obj:`str`) name of the schema to remove.
        database: a :obj:`streamsets.testframework.environment.Database` object.

    """
    database.engine.execute('DROP USER {user} CASCADE'.format(user=schema_name))


def _create_oracle_destination_pipeline(pipeline_builder, pipeline_title, raw_data, table_name, operation,
                                        data_drift=True, auto_create=True, create_as_string=False):
    """Helper function to create and return a pipeline with Oracle Destination
    The Deduplicator assures there is only one ingest to database. The pipeline looks like:
    The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> oracle_destination
                               record_deduplicator >> trash
    """
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')

    oracle_destination = pipeline_builder.add_stage(name=ORACLE_DESTINATION)
    oracle_destination.set_attributes(default_operation=operation,
                                 table_name=table_name,
                                 stage_on_record_error='STOP_PIPELINE',
                                 field_to_column_mapping=[],
                                 enable_data_drift=data_drift,
                                 auto_create_table=auto_create,
                                 create_new_columns_as_string=create_as_string)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> record_deduplicator >> oracle_destination
    record_deduplicator >> trash

    return pipeline_builder.build(title=pipeline_title)


def test_oracle_destination_delete(sdc_builder, sdc_executor, database):
    """Simple Oracle Destination test with DELETE operation.
    The pipeline deletes records from the database and verifies that correct data is in the database.
    Records are deleted if the primary key is matched regardless of other column values.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)
    logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
    connection = database.engine.connect()
    connection.execute(table.insert(), ROWS_IN_DATABASE)

    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_TO_UPDATE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    pipeline = _create_oracle_destination_pipeline(pipeline_builder, 'Oracle Destination Delete', DATA, table_name, 'DELETE')
    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_TO_UPDATE))
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table.select())
        data_from_database = result.fetchall()
        result.close()
        removed_ids = [record['ID'] for record in ROWS_TO_UPDATE]
        assert data_from_database == [(record['NAME'], record['ID']) for record in ROWS_IN_DATABASE if
                                      record['ID'] not in removed_ids]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


def test_oracle_destination_update(sdc_builder, sdc_executor, database):
    """Simple Oracle Destination test with UPDATE operation.
    The pipeline updates records from the database and verifies that the correct data is in the database.
    Records with matching primary key are updated, no action is done for unmatched records.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)
    logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
    connection = database.engine.connect()
    connection.execute(table.insert(), ROWS_IN_DATABASE)

    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_TO_UPDATE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    pipeline = _create_oracle_destination_pipeline(pipeline_builder, 'Oracle Destination Update', DATA, table_name, 'UPDATE')
    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_TO_UPDATE))
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table.select())
        data_from_database = result.fetchall()
        result.close()

        updated_names = {record['ID']: record['NAME'] for record in ROWS_IN_DATABASE}
        updated_names.update({record['ID']: record['NAME'] for record in ROWS_TO_UPDATE})

        actual_data = {(record['NAME'], record['ID']) for record in data_from_database}
        expected_data = {(updated_names[record['ID']], record['ID']) for record in ROWS_IN_DATABASE}

        assert actual_data == expected_data

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


def test_oracle_destination_insert(sdc_builder, sdc_executor, database):
    """Simple Oracle Destination test with INSERT operation.
    The pipeline inserts records into the database and verifies that the correct data is in the database.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)

    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    pipeline = _create_oracle_destination_pipeline(pipeline_builder, 'Oracle Destination Insert', DATA, table_name, 'INSERT')
    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_IN_DATABASE))
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table.select())
        data_from_database = result.fetchall()
        result.close()

        actual_data = {(record['ID'], record['NAME']) for record in data_from_database}
        expected_data = {(record['ID'], record['NAME']) for record in ROWS_IN_DATABASE}

        assert actual_data == expected_data
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


def test_oracle_destination_merge(sdc_builder, sdc_executor, database):
    """Simple Oracle Destination test with MERGE operation.
    The pipeline performs a merge operation and verifies that the correct data is in the database.
    Records with matching primary key are updated, unmatched records are inserted.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)
    logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
    connection = database.engine.connect()
    connection.execute(table.insert(), ROWS_IN_DATABASE)

    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_TO_UPDATE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    pipeline = _create_oracle_destination_pipeline(pipeline_builder, 'Oracle Destination Merge', DATA, table_name, 'MERGE')
    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_TO_UPDATE))
        sdc_executor.stop_pipeline(pipeline)

        result = database.engine.execute(table.select())
        data_from_database = result.fetchall()
        result.close()

        updated_names = {record['ID']: record['NAME'] for record in ROWS_IN_DATABASE}
        for record in ROWS_TO_UPDATE:
            updated_names[record['ID']] = record['NAME']

        updated_data_from_database = [(name, id) for id, name in updated_names.items()]

        actual_data = set(data_from_database)
        expected_data = set(updated_data_from_database)

        # Ensure the data sets match regardless of order
        assert actual_data == expected_data

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


def test_oracle_destination_multitable(sdc_builder, sdc_executor, database):
    """Test for Oracle Destination with multiple destination table. We create 3 tables in the default schema and use an EL
    expression to insert records according to the /table record field.

    Pipeline:
        dev_raw_data_source >> record_deduplicator >> oracle_destination
                               record_deduplicator >> trash

    """
    table1_name = _get_random_name(prefix='stf_table_')
    table2_name = _get_random_name(prefix='stf_table_')
    table3_name = _get_random_name(prefix='stf_table_')

    table1 = _create_table(table1_name, database)
    table2 = _create_table(table2_name, database)
    table3 = _create_table(table3_name, database)

    ROWS = [{'table': table1_name, 'ID': 1, 'NAME': 'Grass'},
            {'table': table2_name, 'ID': 2, 'NAME': 'Fire'},
            {'table': table3_name, 'ID': 3, 'NAME': 'Water'}]

    INPUT_DATA = '\n'.join(json.dumps(rec) for rec in ROWS)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline = _create_oracle_destination_pipeline(pipeline_builder, 'Oracle Destination Multitable Insert', INPUT_DATA,
                                              "${record:value('/table')}", 'INSERT', data_drift=False)

    # Oracle Destination's "Table Name" property is converted to uppercase through the configure_for_environment() method.
    # However EL function names are case-sensitive; we overwrite it afterwards to avoid an EL error.
    pipeline.configure_for_environment(database)
    pipeline[2].set_attributes(table_name="${record:value('/table')}")

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

        assert data1 == [(ROWS[0]['NAME'], ROWS[0]['ID'])]
        assert data2 == [(ROWS[1]['NAME'], ROWS[1]['ID'])]
        assert data3 == [(ROWS[2]['NAME'], ROWS[2]['ID'])]

        result1.close()
        result2.close()
        result3.close()

    finally:
        logger.info('Dropping tables %s, %s, %s in %s database...', table1_name, table2_name, table3_name,
                    database.type)
        table1.drop(database.engine)
        table2.drop(database.engine)
        table3.drop(database.engine)


def test_oracle_destination_multischema(sdc_builder, sdc_executor, database):
    """Test for Oracle Destination in a multischema scenario with a single destination table for each schema. We create 3
    schemas with one table for each, with the same name. Then we use an EL expression to insert records according to
    the /schema record field.

    Pipeline:
        dev_raw_data_source >> record_deduplicator >> oracle_destination
                               record_deduplicator >> trash

    """
    schema1_name = _get_random_schema_name(prefix='stf_schema_')
    schema2_name = _get_random_schema_name(prefix='stf_schema_')
    schema3_name = _get_random_schema_name(prefix='stf_schema_')
    table_name = _get_random_name(prefix='stf_table_')

    _create_schema(schema1_name, database)
    _create_schema(schema2_name, database)
    _create_schema(schema3_name, database)

    table1 = _create_table(table_name, database, schema_name=schema1_name)
    table2 = _create_table(table_name, database, schema_name=schema2_name)
    table3 = _create_table(table_name, database, schema_name=schema3_name)

    ROWS = [{'schema': schema1_name, 'ID': 1, 'NAME': 'Grass'},
            {'schema': schema2_name, 'ID': 2, 'NAME': 'Fire'},
            {'schema': schema3_name, 'ID': 3, 'NAME': 'Water'}]

    INPUT_DATA = '\n'.join(json.dumps(rec) for rec in ROWS)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline = _create_oracle_destination_pipeline(pipeline_builder, 'Oracle Destination Multischema Insert', INPUT_DATA,
                                              table_name, 'INSERT', data_drift=False)

    # Oracle Destination's "Schema Name" property is set through the `database` environment under some circumstances.
    # We overwrite it afterwards for the test.
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

        assert data1 == [(ROWS[0]['NAME'], ROWS[0]['ID'])]
        assert data2 == [(ROWS[1]['NAME'], ROWS[1]['ID'])]
        assert data3 == [(ROWS[2]['NAME'], ROWS[2]['ID'])]

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


def test_oracle_destination_multischema_multitable(sdc_builder, sdc_executor, database):
    """Test an Oracle Destination in a multischema scenario with different destination tables for each schema. We create 3
    schemas with one table for each, with different names. Then we use an EL expressions to insert records according to
    the /schema and /table record fields.

    Pipeline:
        dev_raw_data_source >> record_deduplicator >> oracle_destination
                               record_deduplicator >> trash

    """
    schema1_name = _get_random_schema_name(prefix='stf_schema_')
    schema2_name = _get_random_schema_name(prefix='stf_schema_')
    schema3_name = _get_random_schema_name(prefix='stf_schema_')
    table1_name = _get_random_name(prefix='stf_table_')
    table2_name = _get_random_name(prefix='stf_table_')
    table3_name = _get_random_name(prefix='stf_table_')

    _create_schema(schema1_name, database)
    _create_schema(schema2_name, database)
    _create_schema(schema3_name, database)

    table1 = _create_table(table1_name, database, schema_name=schema1_name)
    table2 = _create_table(table2_name, database, schema_name=schema2_name)
    table3 = _create_table(table3_name, database, schema_name=schema3_name)

    ROWS = [{'schema': schema1_name, 'table': table1_name, 'ID': 1, 'NAME': 'Grass'},
            {'schema': schema2_name, 'table': table2_name, 'ID': 2, 'NAME': 'Fire'},
            {'schema': schema3_name, 'table': table3_name, 'ID': 3, 'NAME': 'Water'}]

    INPUT_DATA = '\n'.join(json.dumps(rec) for rec in ROWS)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline = _create_oracle_destination_pipeline(pipeline_builder, 'Oracle Destination Multischema and Multitable Insert',
                                              INPUT_DATA, "${record:value('/table')}", 'INSERT', data_drift=False)

    # Oracle Destination's "Schema Name" property is set through the `database` environment under some circumstances.
    # We overwrite it afterwards for the test.
    pipeline.configure_for_environment(database)
    pipeline[2].set_attributes(schema_name="${record:value('/schema')}")

    # Oracle Destination's "Table Name" property is converted to uppercase through the configure_for_environment() method.
    # However EL function names are case-sensitive; we overwrite it afterwards to avoid an EL error.
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

        assert data1 == [(ROWS[0]['NAME'], ROWS[0]['ID'])]
        assert data2 == [(ROWS[1]['NAME'], ROWS[1]['ID'])]
        assert data3 == [(ROWS[2]['NAME'], ROWS[2]['ID'])]

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


@pytest.mark.parametrize('data_drift', [True, False])
@pytest.mark.parametrize('auto_create_table', [True, False])
@pytest.mark.parametrize('create_as_string', [True, False])
def test_oracle_destination_data_drift(sdc_builder, sdc_executor, database, data_drift, auto_create_table, create_as_string):
    """Simple Oracle Destination test with INSERT operation.
    The pipeline inserts records into the database and verifies that the correct data is in the database.
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    if data_drift and not auto_create_table:
        _create_empty_table(table_name, database)

    DATA = '\n'.join(json.dumps(rec) for rec in ROWS_IN_DATABASE)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    pipeline = _create_oracle_destination_pipeline(pipeline_builder, f'Oracle Destination Data Drift data_drift={data_drift}-'
                                                                     f'auto_create_table={auto_create_table}-create_as_string={create_as_string}',
                                                   DATA, table_name, 'INSERT',
                                                   data_drift, auto_create_table, create_as_string)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(database))

    try:
        if not data_drift:
            with pytest.raises((StartError, RunError)) as e:
                sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_IN_DATABASE), timeout=300)
            assert 'JDBC_16 - ' in f'{e.value}'
        else:
            sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(ROWS_IN_DATABASE))
            sdc_executor.stop_pipeline(pipeline)

            metadata = sqlalchemy.MetaData()
            table = sqlalchemy.Table(table_name, metadata, autoload=True, extend_existing=True, autoload_with=database.engine)
            result = database.engine.execute(table.select())

            data_from_database = result.fetchall()
            result.close()

            actual_data = {(record['id'], record['name']) for record in data_from_database}
            if create_as_string:
                expected_data = {(str(record['ID']), record['NAME']) for record in ROWS_IN_DATABASE}
            else:
                expected_data = {(record['ID'], record['NAME']) for record in ROWS_IN_DATABASE}

            assert actual_data == expected_data
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        if data_drift:
            table.drop(database.engine)
