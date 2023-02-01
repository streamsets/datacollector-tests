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
import string

import pytest
import sqlalchemy
from streamsets.sdk.utils import Version
from streamsets.testframework.environments.databases import OracleDatabase, SQLServerDatabase, PostgreSqlDatabase, \
    MySqlDatabase, MariaDBDatabase
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


# from CommonDatabaseHeader.java
PRIMARY_KEY_COLUMN_OLD_VALUE = 'jdbc.primaryKey.before'
PRIMARY_KEY_COLUMN_NEW_VALUE = 'jdbc.primaryKey.after'

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


def _create_table(table_name, database, schema_name=None, quote=False):
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
                                 sqlalchemy.Column('name', sqlalchemy.String(32), quote=quote),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True,
                                                   autoincrement=False, quote=quote),
                                 schema=schema_name,
                                 quote=quote)
    else:
        table = sqlalchemy.Table(table_name,
                                 metadata,
                                 sqlalchemy.Column('name', sqlalchemy.String(32), quote=quote),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=quote),
                                 schema=schema_name,
                                 quote=quote)

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


@database
def test_jdbc_lookup_processor(sdc_builder, sdc_executor, database, credential_store):
    """Simple JDBC Lookup processor test.
    Pipeline will enrich records with the 'name' by adding a field as 'FirstName'.
    The pipeline looks like:
        dev_raw_data_source >> jdbc_lookup >> wiretap
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database, quote=True)
    logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
    connection = database.engine.connect()
    connection.execute(table.insert(), ROWS_IN_DATABASE)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(LOOKUP_RAW_DATA),
                                       stop_after_first_batch=True)

    jdbc_lookup = pipeline_builder.add_stage('JDBC Lookup')
    query_str = f'SELECT "name" FROM "{table_name}" WHERE "id" = ${{record:value("/id")}}'
    if type(database) in [MySqlDatabase, MariaDBDatabase]:
        query_str = f'SELECT `name` FROM `{table_name}` WHERE `id` = ${{record:value("/id")}}'
    column_mappings = [dict(dataType='USE_COLUMN_TYPE',
                            columnName='name',
                            field='/FirstName')]
    jdbc_lookup.set_attributes(sql_query=query_str,
                               column_mappings=column_mappings)

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> jdbc_lookup >> wiretap.destination
    pipeline = pipeline_builder.build(title='JDBC Lookup').configure_for_environment(database, credential_store)
    sdc_executor.add_pipeline(pipeline)

    LOOKUP_EXPECTED_DATA = copy.deepcopy(ROWS_IN_DATABASE)
    for record in LOOKUP_EXPECTED_DATA:
        record.pop('id')
        record['FirstName'] = record.pop('name')

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        rows_from_wiretap = [{list(record.field.keys())[1]: list(record.field.values())[1].value}
                             for record in wiretap.output_records]
        assert rows_from_wiretap == LOOKUP_EXPECTED_DATA
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)

        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_tee_processor(sdc_builder, sdc_executor, database):
    """Simple JDBC Tee processor test.
    Pipeline will insert records into database and then pass generated database column 'id' to fields.
    The pipeline looks like:
        dev_raw_data_source >> jdbc_tee >> wiretap
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
                                       raw_data='\n'.join(RAW_DATA),
                                       stop_after_first_batch=True)

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

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> jdbc_tee >> wiretap.destination
    pipeline = pipeline_builder.build(title='JDBC Tee').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify the JDBC Tee processor has got new ids which were generated by database.
        rows_from_wiretap = [{list(item.field.keys())[0]: list(item.field.values())[0].value,
                              list(item.field.keys())[1]: int(list(item.field.values())[1].value)}
                             for item in wiretap.output_records]
        assert rows_from_wiretap == ROWS_IN_DATABASE
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
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
        dev_raw_data_source >> expression evaluator >> jdbc_tee >> wiretap
    """
    if isinstance(database, OracleDatabase):
        pytest.skip('JDBC Tee Processor does not support Oracle')
    if type(database) == SQLServerDatabase:
        pytest.skip('JDBC Tee Processor does not support SQL Server')

    table_name = get_random_string(string.ascii_lowercase, 20)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    DATA = [
        {'operation': 2, 'name': 'Jarcec', 'id': 2},  # delete
        {'operation': 3, 'name': 'Hari', 'id': 3},  # update
        {'operation': 1, 'name': 'Eddie'},  # insert, id will be added by JDBC Tee
        {'operation': 1, 'name': 'Fran'}  # insert, id will be added by JDBC Tee
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

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> expression_evaluator >> jdbc_tee >> wiretap.destination
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

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
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
        name_id_from_output = [(record.field['name'], record.field['id']) for record in wiretap.output_records]
        assert name_id_from_output == [('Jarcec', 2), ('Hari', 3), ('Eddie', 4), ('Fran', 5)]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


DATA_TYPES_MYSQL_ZONED_DATETIME = [
    # Zoned Datetime
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'char(50)', '2020-01-01T10:00:00Z', '2020-01-01T10:00:00Z'),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'varchar(50)', '2020-01-01T10:00:00Z', '2020-01-01T10:00:00Z'),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'binary(20)', b'2020-01-01T10:00:00Z', b'2020-01-01T10:00:00Z'),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'varbinary(50)', b'2020-01-01T10:00:00Z', b'2020-01-01T10:00:00Z'),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'text', '2020-01-01T10:00:00Z', '2020-01-01T10:00:00Z'),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'blob', b'2020-01-01T10:00:00Z', b'2020-01-01T10:00:00Z'),
]
@database('mysql')
@pytest.mark.parametrize('input,converter_type,database_type,expected_legacy_format,expected_default_format',
                         DATA_TYPES_MYSQL_ZONED_DATETIME, ids=[f"{i[1]}-{i[2]}" for i in
                                                               DATA_TYPES_MYSQL_ZONED_DATETIME])
@pytest.mark.parametrize('use_legacy_zoned_datetime_format', [True, False, 'randomString'])
def test_jdbc_tee_processor_mysql_use_legacy_zoned_datetime_format_property(sdc_builder, sdc_executor, input,
                                                                            converter_type, database_type,
                                                                            expected_legacy_format,
                                                                            expected_default_format, database,
                                                                            keep_data,
                                                                            use_legacy_zoned_datetime_format):
    """JDBC Tee processor MySQL
        In the pipeline add a new custom property on the JDBC Tee processor ("useLegacyZonedDatetime") to show the legacy
        zoned DateTime format. The pipeline looks like this:
            origin >> converter >> tee >> wiretap
    """

    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    connection.execute("SET sql_mode=ANSI_QUOTES")

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.stop_after_first_batch = True
    origin.raw_data = json.dumps({"value": input})

    converter = builder.add_stage('Field Type Converter')
    converter.conversion_method = 'BY_FIELD'
    converter.field_type_converter_configs = [{
        'fields': ['/value'],
        'targetType': converter_type,
        'dataLocale': 'en,US',
        'dateFormat': 'YYYY_MM_DD_HH_MM_SS',
        'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
        'scale': 2
    }]

    tee = builder.add_stage('JDBC Tee')
    tee.table_name = table_name
    tee.default_operation = 'INSERT'
    tee.field_to_column_mapping = []
    tee.on_record_error = 'STOP_PIPELINE'
    tee.generated_column_mappings = [{
        'dataType': 'USE_COLUMN_TYPE',
        'columnName': 'id',
        'field': '/id'
    }]
    wiretap = builder.add_wiretap()

    properties = [{'key': 'useLegacyZonedDatetime', 'value': str(use_legacy_zoned_datetime_format)}]
    attributes = {'additional_jdbc_configuration_properties': properties}
    tee.set_attributes(**attributes)

    origin >> converter >> tee >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    # Workarounds for STE,STF specific stuff

    tee.init_query = "SET sql_mode=ANSI_QUOTES"

    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table
        connection.execute(f"""
                CREATE TABLE "{table_name}"(
                    "id" int primary key auto_increment,
                    "value" {database_type} NULL
                )
            """)

        # Run pipeline and read from Elasticsearch to assert
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify returned records
        records = wiretap.output_records
        assert len(records) == 1
        assert records[0].field['id'] == 1

        rs = connection.execute(f'select "id", "value" from "{table_name}"')
        rows = [row for row in rs]
        assert len(rows) == 1

        # Generated key is "1"
        assert rows[0][0] == 1

        # And assert actual value - few corrections for "problematical" types
        actual = rows[0][1]
        if type(actual) == memoryview:
            actual = actual.tobytes()

        expected = expected_legacy_format if use_legacy_zoned_datetime_format is True else expected_default_format
        assert actual == expected

    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f'DROP TABLE "{table_name}"')


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
        dev_raw_data_source >> record_deduplicator >> jdbc_query_executor >= wiretap
                               record_deduplicator >> trash2
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)

    DATA = ['id,name'] + [','.join(str(item) for item in rec.values()) for rec in ROWS_IN_DATABASE]
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(DATA),
                                       stop_after_first_batch=True)

    query_str = f"INSERT INTO {table_name} (name, id) VALUES ('${{record:value('/name')}}', '${{record:value('/id')}}')"

    jdbc_query_executor = pipeline_builder.add_stage('JDBC Query', type='executor')

    if Version(sdc_builder.version) < Version('3.14.0'):
        jdbc_query_executor.set_attributes(sql_query=query_str)
    else:
        jdbc_query_executor.set_attributes(sql_queries=[query_str])

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    wiretap = pipeline_builder.add_wiretap()
    trash2 = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> jdbc_query_executor >= wiretap.destination
    record_deduplicator >> trash2
    pipeline = pipeline_builder.build(title='JDBC Query Executor').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        event_records = wiretap.output_records
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
        dev_raw_data_source >> record_deduplicator >> jdbc_query_executor >= wiretap
                               record_deduplicator >> trash2
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)

    DATA = ['id,name'] + [','.join(str(item) for item in rec.values()) for rec in ROWS_IN_DATABASE]
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(DATA),
                                       stop_after_first_batch=True)

    query_str = f"INSERT INTO {table_name} (name, id) VALUES ('${{record:value('/name')}}', '${{record:value('/id')}}')"

    jdbc_query_executor = pipeline_builder.add_stage('JDBC Query', type='executor')

    jdbc_query_executor.set_attributes(include_query_result_count_in_events=True)

    if Version(sdc_builder.version) < Version('3.14.0'):
        jdbc_query_executor.set_attributes(sql_query=query_str)
    else:
        jdbc_query_executor.set_attributes(sql_queries=[query_str])

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    wiretap = pipeline_builder.add_wiretap()
    trash2 = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> jdbc_query_executor >= wiretap.destination
    record_deduplicator >> trash2
    pipeline = pipeline_builder.build(title='JDBC Query Executor').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        event_records = wiretap.output_records
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
    source.raw_data = 'SOMETHING'

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
    source.table_configs = [{"tablePattern": 'this_table_do_not_exists'}]

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
        dev_raw_data_source >> record_deduplicator >> jdbc_query_executor1 >= jdbc_query_executor2 >= wiretap
                               record_deduplicator >> trash2
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)

    DATA = ['id,name'] + [','.join(str(item) for item in rec.values()) for rec in ROWS_IN_DATABASE]
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(DATA),
                                       stop_after_first_batch=True)

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
    wiretap = pipeline_builder.add_wiretap()
    trash2 = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> jdbc_query_executor1 >= jdbc_query_executor2 >= wiretap.destination
    record_deduplicator >> trash2
    pipeline = pipeline_builder.build(title='JDBC Query Executor').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        event_records = wiretap.output_records
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
        dev_raw_data_source >> record_deduplicator >> jdbc_query_executor >= wiretap
                               record_deduplicator >> trash2
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)

    DATA = ['id,name'] + [','.join(str(item) for item in rec.values()) for rec in ROWS_IN_DATABASE]
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(DATA),
                                       stop_after_first_batch=True)
    invalid_table = "INVALID_TABLE"
    query_str = f"INSERT INTO {invalid_table} (name, id) VALUES ('${{record:value('/name')}}', '${{record:value('/id')}}')"

    jdbc_query_executor = pipeline_builder.add_stage('JDBC Query', type='executor')

    if Version(sdc_builder.version) < Version('3.14.0'):
        jdbc_query_executor.set_attributes(sql_query=query_str)
    else:
        jdbc_query_executor.set_attributes(sql_queries=[query_str])

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    wiretap = pipeline_builder.add_wiretap()
    trash2 = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> jdbc_query_executor >= wiretap.destination
    record_deduplicator >> trash2
    pipeline = pipeline_builder.build(title='JDBC Query Executor').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        event_records = wiretap.output_records
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

    # first, the inserts - they will run in parallel,
    # then all the updates will run sequentially
    # net result is all records should get updated to the (last) new value.
    # otherwise we've failed.
    statements = []
    for rec in ROWS_IN_DATABASE:
        statements.extend([f"INSERT INTO {table_name} (name, id) VALUES ('{rec['name']}', {rec['id']})",
                           f"UPDATE {table_name} SET name = 'bob' WHERE id = {rec['id']}",
                           f"UPDATE {table_name} SET name = 'MERRICK' WHERE id = {rec['id']}"])
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
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3 * len(ROWS_IN_DATABASE))
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

    if isinstance(database, OracleDatabase) and use_multi_row:
        pytest.skip('multi_row is not supported on oracle databases')

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
    origin.record_type = 'NATIVE_OBJECTS'
    origin.user_script = script

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


# SDC-16138: JDBC Lookup Processor is not properly catching UncheckedExecutionException
@database
def test_jdbc_lookup_processor_incorrect_query_for_data(sdc_builder, sdc_executor, database, keep_data):
    table_name = get_random_string(string.ascii_uppercase, 20)
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('key', sqlalchemy.Integer, primary_key=False, quote=True),
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=False, quote=True),
        quote=True
    )

    try:
        logger.info('Creating table %s', table_name)
        table.create(database.engine)

        logger.info('Inserting data into %s', table_name)
        connection = database.engine.connect()
        connection.execute(table.insert(), [
            {"key": 1, "id": 1},
            {"key": 2, "id": 1},
            {"key": 2, "id": 2}
        ])

        builder = sdc_builder.get_pipeline_builder()
        source = builder.add_stage('Dev Raw Data Source')
        source.data_format = 'JSON'
        source.raw_data = '{"key": 1 }\n{"key": 2}\n{"key": 1}\n'
        source.stop_after_first_batch = True

        lookup = builder.add_stage('JDBC Lookup')
        # Query is intentionally correlated to create a SQL error (the correlated subsequery will return two rows
        # for the key=2 which makes the query invalid).
        lookup.sql_query = f"SELECT \"id\" FROM \"{table_name}\" WHERE \"id\" = (select \"id\" FROM \"{table_name}\" WHERE \"key\" = ${{record:value('/key')}})"
        lookup.column_mappings = [dict(dataType='USE_COLUMN_TYPE', columnName='id', field='/id')]

        wiretap = builder.add_wiretap()
        source >> lookup >> wiretap.destination
        pipeline = builder.build().configure_for_environment(database)
        if isinstance(database, MySqlDatabase) or isinstance(database, MariaDBDatabase):
            lookup.init_query = "SET sql_mode=ANSI_QUOTES"
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # We are expecting 2 records that will lookup the id 1
        assert len(wiretap.output_records) == 2
        assert wiretap.output_records[0].field['id'] == 1
        assert wiretap.output_records[1].field['id'] == 1
        # And one error record for the key 2
        assert len(wiretap.error_records) == 1
        assert wiretap.error_records[0].field['key'] == 2
        assert wiretap.error_records[0].header['errorCode'] == 'JDBC_02'
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            table.drop(database.engine)


@sdc_min_version('5.2.0')
@database
@pytest.mark.parametrize('multi_row', [False, True])
def test_jdbc_tee_processor_primary_key_header_update(sdc_builder, sdc_executor, database, multi_row):
    """
    Test to make sure Primary Key updates are handled correctly. We will insert one record, and then
    perform multiple updates to its columns (including Primary Key columns) and check we end up
    having just one row in the table, with the expected values.
    The pipeline looks like:

        dev_raw_data_source >> expression_evaluator >> field_remover >> jdbc_tee >> wiretap
    """
    if isinstance(database, OracleDatabase):
        pytest.skip('JDBC Tee Processor does not support Oracle')
    elif type(database) == SQLServerDatabase:
        pytest.skip('JDBC Tee Processor does not support SQL Server')

    table_name = get_random_string(string.ascii_lowercase, 20)

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
    jdbc_tee = pipeline_builder.add_stage('JDBC Tee')
    # Note that here ids are not inserted. Database generates them automatically.
    jdbc_tee.set_attributes(default_operation='INSERT',
                            field_to_column_mapping=[],
                            generated_column_mappings=[],
                            use_multi_row_operation=multi_row,
                            table_name=table_name)
    if isinstance(database, PostgreSqlDatabase):
        jdbc_tee.set_attributes(enclose_table_name=True)

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> expression_evaluator >> field_remover >> jdbc_tee >> wiretap.destination

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

        # We assert that the pipeline processed 1 insert and 6 updates (the extra output is due to wiretap)
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 7
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 0
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 7 + 1

        # And assert that in the database we end up having just 1 row, with the latest update value
        assert len(data_from_database) == 1
        assert data_from_database == CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY_EXPECTED_DATA

        # And we will also check that the processor generated records with the corresponding updated values
        assert len(wiretap.output_records) == 7
        for record, expected_data in zip(wiretap.output_records, CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY):
            assert len(record.field) == len(expected_data.keys())  # make sure we have the number of fields needed
            for field in record.field:
                assert record.field[field] == expected_data[field]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)
