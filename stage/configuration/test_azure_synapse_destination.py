# Copyright 2023 StreamSets Inc.
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
import pytest
import string

from copy import deepcopy
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import azure, category, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version
from ..utils.utils_azure_synapse import \
    FORMATTED_ROWS_IN_DATABASE_WITH_PARTITION,\
    ROWS_IN_DATABASE_WITH_PARTITION,\
    delete_table,\
    get_synapse_pipeline,\
    stop_pipeline

pytestmark = sdc_min_version('5.5.0')

logger = logging.getLogger(__name__)


@stub
@category('basic')
def test_account_name(sdc_builder, sdc_executor):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'external_authentication_method': 'SHARED_KEY'}])
def test_account_shared_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('basic')
def test_adls_file_system(sdc_builder, sdc_executor):
    pass


@stub
@category('basic')
@pytest.mark.parametrize(
    'stage_attributes',
    [{'internal_authentication_method': 'SERVICE_PRINCIPALS', 'use_alternative_external_authentication_method': True}]
)
def test_application_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize(
    'stage_attributes',
    [{'internal_authentication_method': 'SERVICE_PRINCIPALS', 'use_alternative_external_authentication_method': True}]
)
def test_application_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize(
    'stage_attributes',
    [{'authentication_method': 'AZURE_ACTIVE_DIRECTORY'}, {'authentication_method': 'SQL_SERVER_LOGIN'}]
)
def test_authentication_method(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'auto_create_table': False}, {'auto_create_table': True}])
def test_auto_create_table(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'authentication_method': 'AZURE_ACTIVE_DIRECTORY'}])
def test_azure_active_directory_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('basic')
def test_azure_synapse_name(sdc_builder, sdc_executor):
    pass


@stub
@category('advanced')
def test_binary_default(sdc_builder, sdc_executor):
    pass


@stub
@category('advanced')
def test_bit_default(sdc_builder, sdc_executor):
    pass


@category('basic')
@pytest.mark.parametrize('case_insensitive', [False, True])
def test_case_insensitive(sdc_builder, sdc_executor, azure, case_insensitive):
    """
    Test for Azure Synapse target stage. We do so by running a dev raw data source to
    Azure Synapse destination and then reading data from SQL Server database for assertion.
    Table name case-insensitive option is tested.

    The pipeline looks like:
    dev_raw_data_source >> azure_synapse_destination
    """

    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)

    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'
    synapse_pipeline = get_synapse_pipeline(
        sdc_builder,
        azure,
        ROWS_IN_DATABASE_WITH_PARTITION,
        stage_file_prefix,
        destination_table_name
    )
    synapse_pipeline.stages[1].set_attributes(
        case_insensitive=case_insensitive
    )

    sdc_executor.add_pipeline(synapse_pipeline)
    engine = azure.synapse.engine
    try:
        sdc_executor.start_pipeline(synapse_pipeline).wait_for_finished()

        stmt = f'select * from [{azure.synapse_database_schema}].[{destination_table_name}]'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        assert data_from_database == FORMATTED_ROWS_IN_DATABASE_WITH_PARTITION

        stmt = (f'SELECT name from sys.tables '
                f'WHERE name = \'{destination_table_name}\';')

        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()
        if case_insensitive:
            assert data_from_database == [(destination_table_name.upper(),)]
        else:
            assert data_from_database == [(destination_table_name,)]
    finally:
        try:
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


@stub
@category('advanced')
def test_column_fields_to_ignore(sdc_builder, sdc_executor):
    pass


@stub
@category('advanced')
def test_connection_pool_size(sdc_builder, sdc_executor):
    pass


@stub
@category('basic')
@pytest.mark.parametrize(
    'stage_attributes',
    [
        {
            'create_new_columns_as_varchar': False,
            'enable_data_drift': True
        }, {
            'create_new_columns_as_varchar': True,
            'enable_data_drift': True
        }
    ]
)
def test_create_new_columns_as_varchar(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('basic')
def test_database(sdc_builder, sdc_executor):
    pass


@stub
@category('advanced')
def test_date_default(sdc_builder, sdc_executor):
    pass


@stub
@category('advanced')
def test_datetime_default(sdc_builder, sdc_executor):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'enable_data_drift': False}, {'enable_data_drift': True}])
def test_enable_data_drift(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize(
    'stage_attributes',
    [{'external_authentication_method': 'OAUTH'}, {'external_authentication_method': 'SHARED_KEY'}]
)
def test_external_authentication_method(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize(
    'stage_attributes',
    [{'ignore_fields_with_invalid_types': False}, {'ignore_fields_with_invalid_types': True}]
)
def test_ignore_fields_with_invalid_types(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'ignore_missing_fields': False}, {'ignore_missing_fields': True}])
def test_ignore_missing_fields(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize(
    'stage_attributes',
    [
        {
            'internal_authentication_method': 'AAD_USER',
            'use_alternative_external_authentication_method': True
        }, {
            'internal_authentication_method': 'MANAGED_IDENTITY',
            'use_alternative_external_authentication_method': True
        }, {
            'internal_authentication_method': 'SAS',
            'use_alternative_external_authentication_method': True
        }, {
            'internal_authentication_method': 'SERVICE_PRINCIPALS',
            'use_alternative_external_authentication_method': True
        }, {
            'internal_authentication_method': 'STORAGE_ACCOUNT_KEY',
            'use_alternative_external_authentication_method': True
        }
    ]
)
def test_internal_authentication_method(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('advanced')
def test_numeric_default(sdc_builder, sdc_executor):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize(
    'stage_attributes',
    [
        {'on_record_error': 'DISCARD'},
        {'on_record_error': 'STOP_PIPELINE'},
        {'on_record_error': 'TO_ERROR'}
    ]
)
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@category('basic')
@azure('synapse')
@pytest.mark.parametrize(
    'partition_values',
    [
        ['a', 'b', 'c'],
        ['1', '2', '3'],
        ['0x1', '0x2', '0xA'],
        ['1992-01-01', '1992-02-01', '1992-03-01'],
        ['1992-01-01 00:00:01', '1992-01-01 00:00:02', '1992-01-01 00:00:03'],
        ['$100.00', '$200.00', '$300.00']
    ]
)
def test_partition_values(sdc_builder, sdc_executor, azure, partition_values):
    """
    Test for Azure Synapse target stage. We do so by running a dev raw data source to
    Azure Synapse destination and then reading data from SQL Server database for assertion.
    Different partition values types are tested checking the table partitions in the database.

    The pipeline looks like:
    dev_raw_data_source >> azure_synapse_destination
    """

    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)

    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'
    # The first two rows will have the first value of partition value, the second two rows the second value
    # and the last two the third value.
    ROWS_IN_DATABASE_WITH_PARTITION_copied = deepcopy(ROWS_IN_DATABASE_WITH_PARTITION)
    for row_index in range(0, len(ROWS_IN_DATABASE_WITH_PARTITION_copied)):
        ROWS_IN_DATABASE_WITH_PARTITION_copied[row_index]['partition'] = partition_values[row_index // 2]

    synapse_pipeline = get_synapse_pipeline(
        sdc_builder,
        azure,
        ROWS_IN_DATABASE_WITH_PARTITION_copied,
        stage_file_prefix,
        destination_table_name
    )

    partition_expression = partition_values
    if Version(synapse_pipeline.stages[1].stage_version) > Version('1'):
        partition_expression = ','.join(partition_values)

    synapse_pipeline.stages[1].set_attributes(
        partition_table=True,
        partition_column='partition',
        partition_values=partition_expression
    )

    sdc_executor.add_pipeline(synapse_pipeline)
    engine = azure.synapse.engine
    try:
        sdc_executor.start_pipeline(synapse_pipeline).wait_for_finished()

        stmt = f'select * from [{azure.synapse_database_schema}].[{destination_table_name}]'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        # Assert that the six rows are in the database
        expected_data = [(row['id'], row['name'], row['partition']) for row in ROWS_IN_DATABASE_WITH_PARTITION_copied]
        assert data_from_database == expected_data

        # Query for getting n + 1 partitions in the database for n being the quantity of values
        stmt = (f'SELECT CAST(rv.[value] as varchar(20)) FROM sys.schemas s '
                f'JOIN sys.tables t ON t.[schema_id] = s.[schema_id] '
                f'JOIN sys.partitions p ON p.[object_id] = t.[object_id] '
                f'JOIN sys.indexes i ON i.[object_id] = p.[object_id] AND i.[index_id]= p.[index_id] '
                f'JOIN sys.data_spaces ds ON ds.[data_space_id] = i.[data_space_id] '
                f'LEFT JOIN sys.partition_schemes ps ON ps.[data_space_id] = ds.[data_space_id] '
                f'LEFT JOIN sys.partition_functions pf ON pf.[function_id] = ps.[function_id] '
                f'LEFT JOIN sys.partition_range_values rv ON rv.[function_id] = pf.[function_id] '
                f'AND rv.[boundary_id]= p.[partition_number] '
                f'WHERE p.[index_id] <=1 and t.[name] = \'{destination_table_name}\';')
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: str(row[0]))
        result.close()

        # There are three partition values plus one: None.
        assert data_from_database == sorted(
            [(None,)] + [(str(partition_values[_]),) for _ in range(len(partition_values))],
            key=lambda row: str(row[0]))

    finally:
        try:
            logger.info('Deleting table = %s', destination_table_name)
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


@category('basic')
@azure('synapse')
@pytest.mark.parametrize('partition_boundary', ['UPPER', 'LOWER'])
def test_partition_boundary(sdc_builder, sdc_executor, azure, partition_boundary):
    """
    Test for Azure Synapse target stage. We do so by running a dev raw data source to
    Azure Synapse destination and then reading data from SQL Server database for assertion.
    Partition boundary range right is tested checking the table partitions in the database.

    The pipeline looks like:
    dev_raw_data_source >> azure_synapse_destination
    """

    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)

    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'
    synapse_pipeline = get_synapse_pipeline(
        sdc_builder,
        azure,
        ROWS_IN_DATABASE_WITH_PARTITION,
        stage_file_prefix,
        destination_table_name
    )

    partition_values = ['b', 'c']
    if Version(synapse_pipeline.stages[1].stage_version) > Version('1'):
        partition_values = ','.join(partition_values)

    synapse_pipeline.stages[1].set_attributes(
        partition_table=True,
        partition_boundary=partition_boundary,
        partition_column='partition',
        partition_values=partition_values
    )

    sdc_executor.add_pipeline(synapse_pipeline)
    engine = azure.synapse.engine
    try:
        sdc_executor.start_pipeline(synapse_pipeline).wait_for_finished()

        stmt = f'select * from [{azure.synapse_database_schema}].[{destination_table_name}]'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        assert data_from_database == FORMATTED_ROWS_IN_DATABASE_WITH_PARTITION

        stmt = (f'SELECT pf.[boundary_value_on_right] FROM sys.schemas s '
                f'JOIN sys.tables t ON t.[schema_id] = s.[schema_id] '
                f'JOIN sys.partitions p ON p.[object_id] = t.[object_id] '
                f'JOIN sys.indexes i ON i.[object_id] = p.[object_id] AND i.[index_id]= p.[index_id] '
                f'JOIN sys.data_spaces ds ON ds.[data_space_id] = i.[data_space_id] '
                f'LEFT JOIN sys.partition_schemes ps ON ps.[data_space_id] = ds.[data_space_id] '
                f'LEFT JOIN sys.partition_functions pf ON pf.[function_id] = ps.[function_id] '
                f'LEFT JOIN sys.partition_range_values rv ON rv.[function_id] = pf.[function_id] '
                f'AND rv.[boundary_id]= p.[partition_number] '
                f'WHERE p.[index_id] <=1 and t.[name] = \'{destination_table_name}\';')

        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()
        partition_boundary_right = partition_boundary == 'UPPER'
        assert data_from_database == [(partition_boundary_right,)] * 3

    finally:
        try:
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


@category('basic')
@azure('synapse')
@pytest.mark.parametrize('auto_create_table', [True, False])
def test_auto_create_table(sdc_builder, sdc_executor, azure, auto_create_table):
    """
    Test for Azure Synapse target stage. We do so by running a dev raw data source to
    Azure Synapse destination and then reading data from SQL Server database for assertion.
    Partition table attribute is tested.

    The pipeline looks like:
    dev_raw_data_source >> azure_synapse_destination
    """

    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)

    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'
    synapse_pipeline = get_synapse_pipeline(
        sdc_builder,
        azure,
        ROWS_IN_DATABASE_WITH_PARTITION,
        stage_file_prefix,
        destination_table_name
    )
    synapse_pipeline.stages[1].set_attributes(auto_create_table=auto_create_table)

    engine = azure.synapse.engine

    if not auto_create_table:
        stmt = (f'create table [{azure.synapse_database_schema}].[{destination_table_name}] ('
                f'ID int NOT NULL, NAME varchar(50), PARTITION varchar(2));')
        engine.execute(stmt)

    sdc_executor.add_pipeline(synapse_pipeline)
    try:
        sdc_executor.start_pipeline(synapse_pipeline).wait_for_finished()

        stmt = f'select * from [{azure.synapse_database_schema}].[{destination_table_name}]'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        assert data_from_database == FORMATTED_ROWS_IN_DATABASE_WITH_PARTITION

    finally:
        try:
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


@category('basic')
@azure('synapse')
@pytest.mark.parametrize('partition_table', [False, True])
def test_partition_table(sdc_builder, sdc_executor, azure, partition_table):
    """
    Test for Azure Synapse target stage. We do so by running a dev raw data source to
    Azure Synapse destination and then reading data from SQL Server database for assertion.
    Partition table attribute is tested.

    The pipeline looks like:
    dev_raw_data_source >> azure_synapse_destination
    """

    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)

    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'
    synapse_pipeline = get_synapse_pipeline(
        sdc_builder,
        azure,
        ROWS_IN_DATABASE_WITH_PARTITION,
        stage_file_prefix,
        destination_table_name
    )
    synapse_pipeline.stages[1].set_attributes(
        partition_table=partition_table
    )

    if partition_table:
        partition_values = ['b', 'c']
        if Version(synapse_pipeline.stages[1].stage_version) > Version('1'):
            partition_values = ','.join(partition_values)
        synapse_pipeline.stages[1].set_attributes(
            partition_column='partition',
            partition_values=partition_values
        )

    sdc_executor.add_pipeline(synapse_pipeline)
    engine = azure.synapse.engine
    try:
        sdc_executor.start_pipeline(synapse_pipeline).wait_for_finished()

        stmt = f'select * from [{azure.synapse_database_schema}].[{destination_table_name}]'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        assert data_from_database == FORMATTED_ROWS_IN_DATABASE_WITH_PARTITION

        stmt = (f'Select s.name As SchemaName, t.name, Count(*) As TableName '
                f'From sys.tables t Inner Join sys.schemas s On t.schema_id = s.schema_id '
                f'Inner Join sys.partitions p on p.object_id = t.object_id Where p.index_id In (0, 1) '
                f'Group By s.name, t.name Having Count(*) > 1 '
                f' and t.name=\'{destination_table_name}\' Order By s.name, t.name;')

        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        if partition_table:
            assert len(data_from_database) == 1
            assert data_from_database == [(azure.synapse_database_schema, destination_table_name.upper(), 3)]
        else:
            assert len(data_from_database) == 0

    finally:
        try:
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


@category('basic')
@azure('synapse')
@pytest.mark.parametrize('distribute_table', [True, False])
def test_distribute_table(sdc_builder, sdc_executor, azure, distribute_table):
    """
    Test for Azure Synapse target stage. We do so by running a dev raw data source to
    Azure Synapse destination and then reading data from SQL Server database for assertion.
    Distribute table attribute is tested.

    The pipeline looks like:
    dev_raw_data_source >> azure_synapse_destination
    """

    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)

    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'
    synapse_pipeline = get_synapse_pipeline(
        sdc_builder,
        azure,
        ROWS_IN_DATABASE_WITH_PARTITION,
        stage_file_prefix,
        destination_table_name
    )

    if Version(synapse_pipeline.stages[1].stage_version) <= Version('1'):
        pytest.skip(f'Distribute Table feature is not available in version {synapse_pipeline.stages[1].stage_version}')

    synapse_pipeline.stages[1].set_attributes(
        distribute_table=distribute_table
    )

    if distribute_table:
        distribution_columns = 'partition'
        synapse_pipeline.stages[1].set_attributes(
            distribution_columns=distribution_columns
        )

    sdc_executor.add_pipeline(synapse_pipeline)
    engine = azure.synapse.engine
    try:
        sdc_executor.start_pipeline(synapse_pipeline).wait_for_finished()

        stmt = f'select * from [{azure.synapse_database_schema}].[{destination_table_name}]'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        assert data_from_database == FORMATTED_ROWS_IN_DATABASE_WITH_PARTITION

        stmt = (f'Select p.distribution_policy '
                f'From sys.tables t Inner Join sys.schemas s On t.schema_id = s.schema_id '
                f'Inner Join sys.pdw_table_distribution_properties p on p.object_id = t.object_id Where '
                f't.name=\'{destination_table_name}\' ;')

        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        if distribute_table:
            # 2 = HASH.
            # 4 = ROUND ROBIN.
            assert data_from_database == [(2,)]
        else:
            assert data_from_database == [(4,)]

        stmt = (f'Select c.name, p.distribution_ordinal '
                f'From sys.tables t Inner Join sys.schemas s On t.schema_id = s.schema_id '
                f'Inner Join sys.columns c on t.object_id = c.object_id '
                f'Inner Join sys.pdw_column_distribution_properties p on p.object_id = t.object_id '
                f'and c.column_id=p.column_id Where '
                f't.name=\'{destination_table_name}\' ;')

        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        if distribute_table:
            # 0 = Not a distribution column.
            # 1,2,3  = Order of this column to distribute the parent table.
            assert data_from_database == [('ID', 0), ('NAME', 0), ('PARTITION', 1)]
        else:
            assert data_from_database == [('ID', 0), ('NAME', 0), ('PARTITION', 0)]

    finally:
        try:
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'authentication_method': 'AZURE_ACTIVE_DIRECTORY'}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('advanced')
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@category('basic')
@pytest.mark.parametrize(
    'stage_attributes',
    [{'purge_stage_file_after_loading': False}, {'purge_stage_file_after_loading': True}]
)
def test_purge_stage_file_after_loading(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('advanced')
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@category('advanced')
def test_row_field(sdc_builder, sdc_executor):
    pass


@stub
@category('basic')
@pytest.mark.parametrize(
    'stage_attributes',
    [{'internal_authentication_method': 'SAS', 'use_alternative_external_authentication_method': True}]
)
def test_sas_token(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('basic')
def test_schema(sdc_builder, sdc_executor):
    pass


@stub
@category('basic')
def test_stage_file_prefix(sdc_builder, sdc_executor):
    pass


@stub
@category('basic')
@pytest.mark.parametrize(
    'stage_attributes',
    [{'internal_authentication_method': 'STORAGE_ACCOUNT_KEY', 'use_alternative_external_authentication_method': True}]
)
def test_storage_account_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@category('basic')
@azure('synapse')
@pytest.mark.parametrize('storage_type', ['ADLSG2', 'BLOB'])
def test_storage_type(sdc_builder, sdc_executor, azure, storage_type):
    """
    Test for Azure Synapse target stage. We do so by running a dev raw data source to
    Azure Synapse destination and then reading data from SQL Server database for assertion.
    Storage type attribute is tested.

    The pipeline looks like:
    dev_raw_data_source >> azure_synapse_destination
    """

    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)

    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'
    synapse_pipeline = get_synapse_pipeline(
        sdc_builder,
        azure,
        ROWS_IN_DATABASE_WITH_PARTITION,
        stage_file_prefix,
        destination_table_name
    )
    synapse_pipeline.stages[1].set_attributes(storage_type=storage_type)

    sdc_executor.add_pipeline(synapse_pipeline)
    engine = azure.synapse.engine
    try:
        sdc_executor.start_pipeline(synapse_pipeline).wait_for_finished()

        stmt = f'select * from [{azure.synapse_database_schema}].[{destination_table_name}]'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        assert data_from_database == FORMATTED_ROWS_IN_DATABASE_WITH_PARTITION

    finally:
        try:
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


@stub
@category('basic')
def test_table(sdc_builder, sdc_executor):
    pass


@stub
@category('basic')
@pytest.mark.parametrize(
    'stage_attributes',
    [{'internal_authentication_method': 'SERVICE_PRINCIPALS', 'use_alternative_external_authentication_method': True}]
)
def test_tenant_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('advanced')
def test_time_default(sdc_builder, sdc_executor):
    pass


@stub
@category('basic')
@pytest.mark.parametrize(
    'stage_attributes',
    [
        {'use_alternative_external_authentication_method': False},
        {'use_alternative_external_authentication_method': True}
    ]
)
def test_use_alternative_external_authentication_method(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'authentication_method': 'SQL_SERVER_LOGIN'}])
def test_user(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@category('advanced')
def test_varchar_default(sdc_builder, sdc_executor):
    pass
