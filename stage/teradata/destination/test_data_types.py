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

import pytest
from sqlalchemy import Table

from stage.utils.common import cleanup
from . import (
    pytestmark,
    stage_name,
    teradata_manager,
    LOCAL_STAGING_LOCATION,
    DEFAULT_EXTERNAL_STAGING_LOCATION,
    EXTERNAL_STAGING_LOCATIONS,
    CSV_EXTERNAL_DATA_TYPES,
    FASTLOAD_DATA_TYPES,
    AUTO_CREATED_DATA_TYPES,
    PARQUET_EXTERNAL_DATA_TYPES,
    CREATE_TABLE_DDL_TEMPLATE
)
from stage.utils.data_loading_stages import (
    data_loading_pipeline_handler_creator,
    data_loading_data_type_record_creator
)


def _test_data_types(teradata, cleanup, teradata_manager, stage_name,
                     data_loading_pipeline_handler_creator, data_loading_data_type_record_creator,
                     staging_location, staging_file_format, auto_create_table) -> Table:
    """
        Base test case for data types.
    """
    columns_definition = data_loading_data_type_record_creator.columns_definition
    test_data = data_loading_data_type_record_creator.test_data
    field_type_converter_configs = data_loading_data_type_record_creator.field_type_converter_configs
    expected_data = data_loading_data_type_record_creator.expected_data

    table = teradata_manager.describe_table(columns=())
    if not auto_create_table:
        columns_definition_ddl = ', '.join([f'"{column.name}" {column.type}' for column in columns_definition])
        # we want to create the Table using queries to gain finer control over types
        create_table_ddl = CREATE_TABLE_DDL_TEMPLATE % (teradata.database, table.name, columns_definition_ddl)
        teradata_manager.execute_query(create_table_ddl)

    authorization = teradata_manager.create_authorization()

    teradata_attributes = {
        'staging_file_format': staging_file_format,
        'staging_location': staging_location,
        'table': table.name,
        'authorization_name': authorization.name,
        'auto_create_table': auto_create_table,
        'enable_data_drift': auto_create_table
    }

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = pipeline_handler.dev_raw_data_source_origin([test_data])
    field_type_converter = pipeline_handler.field_type_converter(field_type_converter_configs)
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> field_type_converter >> teradata_destination

    pipeline_handler.build(teradata).run()

    # and then retrieve the definition again
    table = teradata_manager.populate_table(table)
    data_from_database = teradata_manager.select_from_table(table)

    assert data_from_database == expected_data

    return table


@pytest.mark.parametrize('staging_location', EXTERNAL_STAGING_LOCATIONS)
@pytest.mark.parametrize('staging_file_format, data_types', [
    ['CSV', CSV_EXTERNAL_DATA_TYPES],
    ['PARQUET', PARQUET_EXTERNAL_DATA_TYPES]
])
def test_external_data_types(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                             data_loading_pipeline_handler_creator, data_loading_data_type_record_creator,
                             staging_location, staging_file_format, data_types):
    """
        Assert that all supported data types work as expected. Note that we just do the queries to
        bypass sqlalchemy types mapping.
    """
    _test_data_types(teradata, cleanup, teradata_manager, stage_name,
                     data_loading_pipeline_handler_creator, data_loading_data_type_record_creator,
                     staging_location, staging_file_format, False)


@pytest.mark.parametrize('staging_location', LOCAL_STAGING_LOCATION)
@pytest.mark.parametrize('data_types', [FASTLOAD_DATA_TYPES])
def test_fastload_data_types(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                             data_loading_pipeline_handler_creator, data_loading_data_type_record_creator,
                             staging_location, data_types):
    """
        Assert that all supported data types work as expected. Note that we just do the queries to
        bypass sqlalchemy types mapping. FastLoad only supports CSV staging.
    """
    _test_data_types(teradata, cleanup, teradata_manager, stage_name,
                     data_loading_pipeline_handler_creator, data_loading_data_type_record_creator,
                     staging_location, 'CSV', False)


@pytest.mark.parametrize('staging_location', DEFAULT_EXTERNAL_STAGING_LOCATION)
@pytest.mark.parametrize('data_types', [AUTO_CREATED_DATA_TYPES])
def test_auto_create_data_types(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                                data_loading_pipeline_handler_creator, data_loading_data_type_record_creator,
                                staging_location, data_types):
    """
        Note that we just test it using Azure. We do not need to do it against every staging, but
        given that FastLoad does not support some types, we do it with Azure. Using CSV as it supports
        every data type, but table created if using Parquet is the same.
    """
    table = _test_data_types(teradata, cleanup, teradata_manager, stage_name,
                             data_loading_pipeline_handler_creator, data_loading_data_type_record_creator,
                             staging_location, 'CSV', True)

    database_column_definitions = teradata_manager.column_definitions(table)
    for database_column_definition, data_type in zip(database_column_definitions, data_types):
        # instead of == we use 'in', to avoid extra formatting comparison (DECIMAL in DECIMAL(5))
        assert str(database_column_definition) in str(data_type.database_type)
