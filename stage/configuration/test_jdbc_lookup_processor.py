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
import copy
import datetime
import logging
import string

import pytest
import sqlalchemy
from sqlalchemy.dialects import postgresql

from streamsets import sdk
from streamsets.sdk.utils import Version
from streamsets.testframework.decorators import stub
from streamsets.testframework.environments.databases import OracleDatabase, SQLServerDatabase, MySqlDatabase, \
    MariaDBDatabase
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

LOOKUP_TABLE_DATA = [
    {'id': 1, 'dept': 'mt', 'name': 'Arvind'},
    {'id': 2, 'dept': 'mt', 'name': 'Girish'},
    {'id': 3, 'dept': 'extra', 'name': 'Dima'}
]

@stub
def test_additional_jdbc_configuration_properties(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'auto_commit': False}, {'auto_commit': True}])
def test_auto_commit(sdc_builder, sdc_executor, stage_attributes):
    pass


@database
@sdc_min_version('3.22.0')
@pytest.mark.parametrize('column_type, lookup_data',
                         [('BigInteger', 12345),
                          ('Boolean', False),
                          ('Date', datetime.datetime(2012, 9, 9, 0, 0)),
                          ('DateTime', datetime.datetime(2019, 2, 5, 23, 59, 59)),
                          ('Enum', 'happy'),
                          ('Float', 123.45),
                          ('Integer', 12345),
                          ('LargeBinary', b'data'),
                          ('Numeric', 122345),
                          ('SmallInteger', 12),
                          ('String', 'data'),
                          ('Text', 'data'),
                          ('Unicode', u'data'),
                          ('UnicodeText', u'data')])
@pytest.mark.parametrize('existing_column_name', [True, False])
def test_column_mappings(sdc_builder, sdc_executor, database, credential_store, column_type, existing_column_name,
                         lookup_data):
    """Simple JDBC Lookup processor test.
    Pipeline would enrich records with the 'field' by adding a field as 'FirstName'.
    This test just validates that the columnName from Column Mapping is well configured (except for String type,
    that it also runs).
    The pipeline looks like:
        dev_raw_data_source >> jdbc_lookup >> wiretap
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    name_type = getattr(sqlalchemy, column_type)
    database_data = [{'id': 1, 'name': lookup_data}]
    if column_type in {'String', 'Unicode'}:
        name_type = name_type(32)
    elif column_type in {'Enum'}:
        name_type = name_type('happy', 'sad', name="mood_enum")

    try:
        table = _create_table(table_name, database, None, name_type)
    except sqlalchemy.exc.ProgrammingError as error:
        logger.info(f'Enum already exists: {error}')
        # if enum exists in the database we set the parameter create_type in False
        name_type = postgresql.ENUM('happy', 'sad', name="mood_enum", create_type=False)
        table = _create_table(table_name, database, None, name_type)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(LOOKUP_RAW_DATA),
                                       stop_after_first_batch=True)

    jdbc_lookup = pipeline_builder.add_stage('JDBC Lookup')
    query_str = f'SELECT "name" as "columnName" FROM "{table_name}" WHERE "id" = 1'
    if type(database) in [MySqlDatabase, MariaDBDatabase]:
        query_str = f'SELECT `name` as `columnName` FROM `{table_name}` WHERE `id` = 1'
    column_mappings = [dict(dataType='USE_COLUMN_TYPE',
                            columnName='columnName' if existing_column_name else 'notAColumnName',
                            field='/LookupColumn')]
    jdbc_lookup.set_attributes(sql_query=query_str,
                               column_mappings=column_mappings)

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> jdbc_lookup >> wiretap.destination
    pipeline = pipeline_builder.build(title='JDBC Lookup').configure_for_environment(database, credential_store)
    sdc_executor.add_pipeline(pipeline)
    try:
        logger.info('Adding %s rows into %s database ...', len(ROWS_IN_DATABASE), database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), database_data)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        if existing_column_name:
            # all fields are lookup, and removed by the lookup data
            expected_data = [{str(row['id']): lookup_data} for row in ROWS_IN_DATABASE]
            if type(database) == OracleDatabase and column_type == 'Float':
                rows_from_wiretap = [{record.field['id']: float(record.field['LookupColumn'].value)} for record in
                                     wiretap.output_records]
            else:
                rows_from_wiretap = [{record.field['id']: record.field['LookupColumn']} for record in
                                     wiretap.output_records]
            assert rows_from_wiretap == expected_data
        else:
            pytest.fail("Should not reach as Start Error should have been raised")
    except (sdk.exceptions.StartError, sdk.exceptions.StartingError) as e:
        if not existing_column_name:
            error_code = 'JDBC_95' if Version(sdc_executor.version) < Version('5.8.0') else 'JDBC_INIT_33'
            assert error_code in str(e.args[0])
        else:
            # should never reach
            raise e
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        if connection is not None:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)
            connection.close()

@database
@sdc_min_version('3.22.0')
@pytest.mark.parametrize('validate_column_mappings', [True, False])
@pytest.mark.parametrize('correct_column_name_config', [True, False])
@pytest.mark.parametrize('table_type', ['known', 'el_var', 'wrong'])
def test_validate_column_mappings(sdc_builder, sdc_executor, database, credential_store, validate_column_mappings,
                                  correct_column_name_config, table_type):
    """Test validate column mapping checkbox
    The pipeline looks like:
        dev_raw_data_source >> jdbc_lookup >> trash
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    if correct_column_name_config:
        column_name_config = 'name'
    else:
        column_name_config = 'notAColumnName'

    table = _create_table(table_name, database, None)

    if table_type == 'known':
        if type(database) in [MySqlDatabase, MariaDBDatabase]:
            table_name = f'`{table_name}`'
        else:
            table_name = f'"{table_name}"'
    elif table_type == 'el_var':
        table_name = "${table_name}"
    elif table_type == 'wrong':
        if type(database) in [MySqlDatabase, MariaDBDatabase]:
            table_name = f'`wrong_table_name`'
        else:
            table_name = f'"wrong_table_name"'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(LOOKUP_RAW_DATA),
                                       stop_after_first_batch=True)

    jdbc_lookup = pipeline_builder.add_stage('JDBC Lookup')
    if type(database) in [MySqlDatabase, MariaDBDatabase]:
        query_str = f'SELECT `name` FROM {table_name} WHERE `id` = 1'
    else:
        query_str = f'SELECT "name" FROM {table_name} WHERE "id" = 1'
    column_mappings = [dict(dataType='USE_COLUMN_TYPE',
                            columnName=column_name_config,
                            field='/FirstName')]
    jdbc_lookup.set_attributes(sql_query=query_str,
                               column_mappings=column_mappings,
                               validate_column_mappings=validate_column_mappings)

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> jdbc_lookup >> wiretap.destination
    pipeline = pipeline_builder.build(title='JDBC Lookup').configure_for_environment(database, credential_store)
    sdc_executor.add_pipeline(pipeline)
    try:
        # Start error should be raised if getting validated
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        if validate_column_mappings and not correct_column_name_config and table_type != 'el_var':
            # Should never reach
            pytest.fail('Start pipeline should have failed, but did not')

    except sdk.exceptions.StartError as e:
        if table_type == 'wrong':
            error_code = 'JDBC_97' if Version(sdc_executor.version) < Version('5.8.0') else 'JDBC_INIT_34'
            assert error_code in str(e.args[0])
        elif validate_column_mappings and not correct_column_name_config:
            error_code = 'JDBC_95' if Version(sdc_executor.version) < Version('5.8.0') else 'JDBC_INIT_33'
            assert error_code in str(e.args[0])
        else:
            # should never reach
            pytest.fail(e.args[0])
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@stub
def test_connection_health_test_query(sdc_builder, sdc_executor):
    pass


@stub
def test_connection_timeout_in_seconds(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': False}, {'enable_local_caching': True}])
def test_enable_local_caching(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enforce_read_only_connection': False},
                                              {'enforce_read_only_connection': True}])
def test_enforce_read_only_connection(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True,
                                               'eviction_policy_type': 'EXPIRE_AFTER_ACCESS'},
                                              {'enable_local_caching': True,
                                               'eviction_policy_type': 'EXPIRE_AFTER_WRITE'}])
def test_eviction_policy_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True}])
def test_expiration_time(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_idle_timeout_in_seconds(sdc_builder, sdc_executor):
    pass


@stub
def test_init_query(sdc_builder, sdc_executor):
    pass


@database
def test_jdbc_connection_string(sdc_builder, sdc_executor, database, credential_store):
    """
    The pipeline looks like:
        dev_raw_data_source >> jdbc_lookup >> trash
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    raw_data = LOOKUP_RAW_DATA

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(raw_data),
                                       stop_after_first_batch=True)

    jdbc_lookup = pipeline_builder.add_stage('JDBC Lookup')
    query_str = f'SELECT `name` FROM {table_name} WHERE `id` = -1'
    jdbc_lookup.set_attributes(sql_query=query_str,
                               column_mappings=[])

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> jdbc_lookup >> trash
    pipeline = pipeline_builder.build(title='JDBC Lookup Connections String').configure_for_environment(database, credential_store)
    pipeline.stages[1].set_attributes(jdbc_connection_string="bad connection string")
    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(Exception) as error:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
    if Version(sdc_executor.version) >= Version('5.8.0'):
        assert "JDBC_INIT_00" in error.value.message, f'Expected a JDBC_INIT_00 error, got "{error.value.message}" instead'
    else:
        assert "JDBC_06" in error.value.message, f'Expected a JDBC_06 error, got "{error.value.message}" instead'


@stub
def test_jdbc_driver_class_name(sdc_builder, sdc_executor):
    pass


@stub
def test_max_blob_size_in_bytes(sdc_builder, sdc_executor):
    pass


@stub
def test_max_clob_size_in_characters(sdc_builder, sdc_executor):
    pass


@stub
def test_max_connection_lifetime_in_seconds(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True}])
def test_maximum_entries_to_cache(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_maximum_pool_size(sdc_builder, sdc_executor):
    pass


@stub
def test_minimum_idle_connections(sdc_builder, sdc_executor):
    pass


@database
@pytest.mark.parametrize('missing_values_behavior', ['PASS_RECORD_ON', 'SEND_TO_ERROR'])
def test_missing_values_behavior(
        sdc_builder,
        sdc_executor,
        database,
        missing_values_behavior,
        credential_store,
        keep_data
):
    """
    The pipeline looks like:
        dev_raw_data_source >> jdbc_lookup >> trash
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_and_populate_lookup_table(table_name, database)
    raw_data = LOOKUP_RAW_DATA

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(raw_data),
                                       stop_after_first_batch=True)

    jdbc_lookup = pipeline_builder.add_stage('JDBC Lookup')
    if type(database) in [MySqlDatabase, MariaDBDatabase]:
        query_str = f'SELECT `name` FROM `{table_name}` WHERE `id` = -1'
    else:
        query_str = f'SELECT "name" FROM "{table_name}" WHERE "id" = -1'
    jdbc_lookup.set_attributes(sql_query=query_str,
                               missing_values_behavior=missing_values_behavior,
                               column_mappings=[])

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> jdbc_lookup >> wiretap.destination
    pipeline = pipeline_builder.build(title='JDBC Lookup Missing Values')\
        .configure_for_environment(database, credential_store)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records
        error_records = wiretap.error_records
        if missing_values_behavior == 'PASS_RECORD_ON':
            assert len(output_records) == len(raw_data) - 1, 'Wrong numbers of records'
            assert len(error_records) == 0, 'Wrong numbers of error records'
        elif missing_values_behavior == 'SEND_TO_ERROR':
            assert len(output_records) == 0, 'Wrong numbers of records'
            assert len(error_records) == len(raw_data) - 1, 'Wrong numbers of error records'
        else:
            pytest.fail(f'Invalid missing_values_behavior attribute: {missing_values_behavior}')
    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)


@database
@pytest.mark.parametrize('stage_attributes', [{'multiple_values_behavior': 'ALL_AS_LIST'},
                                              {'multiple_values_behavior': 'FIRST_ONLY'},
                                              {'multiple_values_behavior': 'SPLIT_INTO_MULTIPLE_RECORDS'}])
def test_multiple_values_behavior(sdc_builder, sdc_executor, database, stage_attributes):
    """Ensure that Multiple Values Behavior works as expected for all the options."""
    if isinstance(database, OracleDatabase):
        pytest.skip('This test does not support oracle and its upper casing of column names.')

    # Create the lookup table
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_and_populate_lookup_table(table_name, database)

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.set_attributes(data_format='JSON',
                          raw_data='{"dept": "mt"}',
                          stop_after_first_batch = True)

    lookup = builder.add_stage('JDBC Lookup')
    query_str = f"SELECT name FROM {table_name} WHERE dept = '${{record:value('/dept')}}' ORDER BY id ASC"
    column_mappings = [dict(dataType='USE_COLUMN_TYPE', columnName='name', field='/name')]
    lookup.set_attributes(sql_query=query_str, column_mappings=column_mappings, **stage_attributes)

    wiretap = builder.add_wiretap()
    source >> lookup >> wiretap.destination
    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output = wiretap.output_records

        if stage_attributes['multiple_values_behavior'] == 'FIRST_ONLY':
            assert len(output) == 1
            assert output[0].field['name'] == 'Arvind'
        if stage_attributes['multiple_values_behavior'] == 'ALL_AS_LIST':
            assert len(output) == 1
            assert output[0].field['name'] == ['Arvind', 'Girish']
        if stage_attributes['multiple_values_behavior'] == 'SPLIT_INTO_MULTIPLE_RECORDS':
            assert len(output) == 2
            assert output[0].field['name'] == 'Arvind'
            assert output[1].field['name'] == 'Girish'
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True, 'retry_on_missing_value': False},
                                              {'enable_local_caching': True, 'retry_on_missing_value': True}])
def test_retry_on_missing_value(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_sql_query(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True, 'time_unit': 'DAYS'},
                                              {'enable_local_caching': True, 'time_unit': 'HOURS'},
                                              {'enable_local_caching': True, 'time_unit': 'MICROSECONDS'},
                                              {'enable_local_caching': True, 'time_unit': 'MILLISECONDS'},
                                              {'enable_local_caching': True, 'time_unit': 'MINUTES'},
                                              {'enable_local_caching': True, 'time_unit': 'NANOSECONDS'},
                                              {'enable_local_caching': True, 'time_unit': 'SECONDS'}])
def test_time_unit(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'transaction_isolation': 'DEFAULT'},
                                              {'transaction_isolation': 'TRANSACTION_READ_COMMITTED'},
                                              {'transaction_isolation': 'TRANSACTION_READ_UNCOMMITTED'},
                                              {'transaction_isolation': 'TRANSACTION_REPEATABLE_READ'},
                                              {'transaction_isolation': 'TRANSACTION_SERIALIZABLE'}])
def test_transaction_isolation(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': False}, {'use_credentials': True}])
def test_use_credentials(sdc_builder, sdc_executor, stage_attributes):
    pass


@database
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_username(sdc_builder, sdc_executor, database, credential_store, stage_attributes):
    """
    The pipeline looks like:
        dev_raw_data_source >> jdbc_lookup >> trash
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    raw_data = LOOKUP_RAW_DATA

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(raw_data),
                                       stop_after_first_batch=True)

    jdbc_lookup = pipeline_builder.add_stage('JDBC Lookup')
    query_str = f'SELECT `name` FROM {table_name} WHERE `id` = -1'
    jdbc_lookup.set_attributes(sql_query=query_str,
                               use_credentials=stage_attributes['use_credentials'],
                               column_mappings=[])

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> jdbc_lookup >> trash
    pipeline = pipeline_builder.build(title='JDBC Lookup Username').configure_for_environment(database, credential_store)
    pipeline.stages[1].set_attributes(username="foo")
    pipeline.stages[1].set_attributes(password="bar")
    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(Exception) as error:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
    if Version(sdc_executor.version) == Version('5.8.0'):
        assert "JDBC_INIT_01" in error.value.message, f'Expected a JDBC_INIT_01 error, got "{error.value.message}" instead'
    elif Version(sdc_executor.version) >= Version('5.9.0'):
        assert "JDBC_INIT_48" in error.value.message, f'Expected a JDBC_INIT_48 error, got "{error.value.message}" instead'
    else:
        assert "JDBC_06" in error.value.message, f'Expected a JDBC_06 error, got "{error.value.message}" instead'


def _create_and_populate_lookup_table(name, database):
    """Create common lookup table and fill it with data."""
    table = sqlalchemy.Table(name,
                             sqlalchemy.MetaData(),
                             sqlalchemy.Column('id', sqlalchemy.Integer, quote=True),
                             sqlalchemy.Column('dept', sqlalchemy.String(32), quote=True),
                             sqlalchemy.Column('name', sqlalchemy.String(32), quote=True),
                             quote=True
                             )
    logger.info('Creating table %s in %s database ...', name, database.type)
    table.create(database.engine)

    logger.info('Adding %s rows into %s database ...', len(LOOKUP_TABLE_DATA), database.type)
    connection = database.engine.connect()
    connection.execute(table.insert(), LOOKUP_TABLE_DATA)
    connection.close()
    
    return table


def _create_table(table_name, database, schema_name=None, name_type=sqlalchemy.String(32)):
    """Helper function to create a table with two columns: id (int, PK) and name.

    Args:
        table_name: (:obj:`str`) the name for the new table.
        database: a :obj:`streamsets.testframework.environment.Database` object.
        name_type: sqlalchemy type
        schema_name: (:obj:`str`, optional) when provided, create the new table in a specific schema; otherwise,
            the default schema for the engine’s database connection is used.

    Return:
        The new table as a sqlalchemy.Table object.

    """
    metadata = sqlalchemy.MetaData()
    # quote=True makes the names case sensitive
    if type(database) == SQLServerDatabase:
        table = sqlalchemy.Table(table_name,
                                 metadata,
                                 sqlalchemy.Column('name', name_type, quote=True),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True,
                                                   autoincrement=False, quote=True),
                                 schema=schema_name,
                                 quote=True)
    else:
        table = sqlalchemy.Table(table_name,
                                 metadata,
                                 sqlalchemy.Column('name', name_type, quote=True),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
                                 schema=schema_name,
                                 quote=True)

    logger.info('Creating table %s in %s database ...', table_name, database.type)
    table.create(database.engine)
    return table

@database
def test_validation_for_default_value(sdc_builder, sdc_executor, database, credential_store):
    """
    The pipeline looks like:
        dev_raw_data_source >> jdbc_lookup >> trash
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    raw_data = LOOKUP_RAW_DATA

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(raw_data),
                                       stop_after_first_batch=True)

    jdbc_lookup = pipeline_builder.add_stage('JDBC Lookup')
    query_str = f'SELECT `name` FROM {table_name} WHERE `id` = -1'
    column_mappings = [dict(defaultValue='Albert',
                            columnName="name",
                            field='/FirstName')]
    jdbc_lookup.set_attributes(sql_query=query_str,
                               column_mappings=column_mappings)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> jdbc_lookup >> trash
    pipeline = pipeline_builder.build(title='JDBC Lookup Default Value').configure_for_environment(database, credential_store)
    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(Exception) as error:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

    error_code = 'JDBC_53' if Version(sdc_executor.version) < Version('5.8.0') else 'JDBC_INIT_18'
    assert error_code in error.value.message, f'Expected a {error_code} error, got "{error.value.message}" instead'


@database
def test_worng_data_type_default_value(sdc_builder, sdc_executor, database, credential_store):
    """
    The pipeline looks like:
        dev_raw_data_source >> jdbc_lookup >> trash
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    raw_data = LOOKUP_RAW_DATA

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(raw_data),
                                       stop_after_first_batch=True)

    jdbc_lookup = pipeline_builder.add_stage('JDBC Lookup')
    query_str = f'SELECT `name` FROM {table_name} WHERE `id` = -1'
    column_mappings = [dict(defaultValue='Albert',
                            dataType='INTEGER',
                            columnName="name",
                            field='/FirstName')]
    jdbc_lookup.set_attributes(sql_query=query_str,
                               column_mappings=column_mappings)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> jdbc_lookup >> trash
    pipeline = pipeline_builder.build(title='JDBC Lookup Data Type Default Value').configure_for_environment(database, credential_store)
    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(Exception) as error:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

    error_code = 'JDBC_410' if Version(sdc_executor.version) < Version('5.8.0') else 'JDBC_INIT_41'
    assert error_code in error.value.message, f'Expected a {error_code} error, got "{error.value.message}" instead'


@database
def test_validation_for_datetime_default_value(sdc_builder, sdc_executor, database, credential_store):
    """
    The pipeline looks like:
        dev_raw_data_source >> jdbc_lookup >> trash
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    raw_data = LOOKUP_RAW_DATA

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(raw_data),
                                       stop_after_first_batch=True)

    jdbc_lookup = pipeline_builder.add_stage('JDBC Lookup')
    query_str = f'SELECT `name` FROM {table_name} WHERE `id` = -1'
    column_mappings = [dict(defaultValue='1-1-1',
                            dataType='DATETIME',
                            columnName="date",
                            field='/dateField')]
    jdbc_lookup.set_attributes(sql_query=query_str,
                               column_mappings=column_mappings)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> jdbc_lookup >> trash
    pipeline = pipeline_builder.build(title='JDBC Lookup DateTime').configure_for_environment(database, credential_store)
    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(Exception) as error:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

    error_code = 'JDBC_56' if Version(sdc_executor.version) < Version('5.8.0') else 'JDBC_INIT_20'
    assert error_code in error.value.message, f'Expected a {error_code} error, got "{error.value.message}" instead'
