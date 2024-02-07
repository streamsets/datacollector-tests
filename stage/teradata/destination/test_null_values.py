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
    CREATE_TABLE_DDL_TEMPLATE,
    LOCAL_STAGING_LOCATION,
    DEFAULT_EXTERNAL_STAGING_LOCATION,
    EXTERNAL_STAGING_LOCATIONS,
    FASTLOAD_DATA_TYPES,
    CSV_EXTERNAL_DATA_TYPES,
    PARQUET_EXTERNAL_DATA_TYPES,
    AUTO_CREATED_DATA_TYPES
)
from stage.utils.data_loading_stages import (
    data_loading_pipeline_handler_creator,
    data_loading_data_type_record_creator
)


def _test_null_values(teradata, cleanup, teradata_manager, stage_name,
                      data_loading_pipeline_handler_creator, data_loading_data_type_record_creator,
                      staging_location, staging_file_format, auto_create_table) -> Table:
    """
        Base test case for null values, similar to data types but setting every value to NULL after conversion.
        Note that nulls can be troublesome when generating the CSV: the apache library we use will mark the first
        column as '' if QUOTED is selected. For example, for a {NULL, NULL} row, we will have {'',}. If the first
        column is a String, there is no NULL but an empty String. But for an Integer, for example, it can fail (as
        it is actually an empty string). Using ESCAPED mode for the tests, so all columns can be NULL without issues.

        This test is similar to test_data_types, but using NULL values.
    """
    columns_definition = data_loading_data_type_record_creator.columns_definition
    test_data = data_loading_data_type_record_creator.test_data
    field_type_converter_configs = data_loading_data_type_record_creator.field_type_converter_configs
    expected_data = data_loading_data_type_record_creator.expected_null_data

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
        'quoting_mode': 'ESCAPED',
        'auto_create_table': auto_create_table,
        'enable_data_drift': auto_create_table
    }

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = pipeline_handler.dev_raw_data_source_origin([test_data])
    field_type_converter = pipeline_handler.field_type_converter(field_type_converter_configs)
    null_field_replacer = pipeline_handler.null_field_replacer([column.name for column in columns_definition])
    decimal_precision_expression_evaluator = pipeline_handler.decimal_precision_expression_evaluator(
        [column.name for column in columns_definition if 'DECIMAL' in column.type])
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> field_type_converter >> null_field_replacer >> decimal_precision_expression_evaluator >> teradata_destination

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
def test_external_null_values(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                              data_loading_pipeline_handler_creator, data_loading_data_type_record_creator,
                              staging_location, staging_file_format, data_types):
    """
        Assert that all supported data types work as expected. Note that we just do the queries to
        bypass sqlalchemy types mapping.
    """
    _test_null_values(teradata, cleanup, teradata_manager, stage_name,
                      data_loading_pipeline_handler_creator, data_loading_data_type_record_creator,
                      staging_location, staging_file_format, False)


@pytest.mark.parametrize('staging_location', LOCAL_STAGING_LOCATION)
@pytest.mark.parametrize('data_types', [FASTLOAD_DATA_TYPES])
def test_fastload_null_values(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                              data_loading_pipeline_handler_creator, data_loading_data_type_record_creator,
                              staging_location, data_types):
    """
        Assert null values work as expected for each data type.
    """
    _test_null_values(teradata, cleanup, teradata_manager, stage_name,
                      data_loading_pipeline_handler_creator, data_loading_data_type_record_creator,
                      staging_location, 'CSV', False)


@pytest.mark.parametrize('staging_location', DEFAULT_EXTERNAL_STAGING_LOCATION)
@pytest.mark.parametrize('data_types', [AUTO_CREATED_DATA_TYPES])
def test_auto_create_null_values(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                                 data_loading_pipeline_handler_creator, data_loading_data_type_record_creator,
                                 staging_location, data_types):
    """
        Note that we just test it using Azure. We do not need to do it against every staging, but
        given that FastLoad does not support some types, we do it with Azure. Using CSV as it supports
        every data type, but table created if using Parquet is the same.
    """
    table = _test_null_values(teradata, cleanup, teradata_manager, stage_name,
                              data_loading_pipeline_handler_creator, data_loading_data_type_record_creator,
                              staging_location, 'CSV', True)

    database_column_definitions = teradata_manager.column_definitions(table)
    for database_column_definition, data_type in zip(database_column_definitions, data_types):
        # instead of == we use 'in', to avoid extra formatting comparison (DECIMAL in DECIMAL(5))
        assert str(database_column_definition) in str(data_type.database_type)