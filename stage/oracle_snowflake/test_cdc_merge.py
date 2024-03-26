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
from copy import deepcopy

from . import (
    pytestmark,
    create_stage,
    ALL_STAGING_LOCATIONS,
    INTERNAL_STAGING_LOCATION,
    pipeline_handler_creator,
    record_merger,
    oracle_stage_name,
    snowflake_stage_name,
    database_sql_connection_manager,
    snowflake_sql_connection_manager
)
from stage.utils.common import cleanup
from stage.utils.utils_oracle import (
    MIN_ORACLE_VERSION,
    DefaultConnectionParameters,
    DefaultTableParameters,
    DefaultStartParameters
)
from stage.utils.data_loading_stages import (
    SIMPLE_ROWS,
    CDC_DATABASE_COLUMNS,
    CDC_TEST_CASES,
    PRIMARY_KEY_COLUMNS,
    OPERATION,
    CDC_OPERATIONS,
)
from sqlalchemy import (
    Column,
    Integer
)


@pytest.mark.parametrize('staging_location', ALL_STAGING_LOCATIONS)
@pytest.mark.parametrize('primary_key_location', ['HEADER', 'TABLE'])
def test_cdc_merge(sdc_builder, sdc_executor, database, snowflake, cleanup,
                   database_sql_connection_manager, snowflake_sql_connection_manager, snowflake_stage_name,
                   oracle_stage_name, pipeline_handler_creator, staging_location, primary_key_location):
    """
        Assert that some rows are properly loaded when and using Merge CDC Data.
    """
    test_data = SIMPLE_ROWS
    primary_key_columns = ['id']

    oracle_table = database_sql_connection_manager.create_table()
    snowflake_table = snowflake_sql_connection_manager.create_table()
    snowflake_stage = create_stage(snowflake, staging_location)

    oracle_cdc_attributes = {
        **DefaultConnectionParameters(database)
          | DefaultTableParameters(oracle_table.name)
          | DefaultStartParameters(database)
    }

    table_key_columns = [{
        'keyColumns': primary_key_columns,
        'table': snowflake_table.name
    }]

    snowflake_attributes = {
        'stage_location': staging_location,
        'table': snowflake_table.name,
        'snowflake_stage_name': snowflake_stage,
        'processing_cdc_data': True,
        'primary_key_location': primary_key_location,
        'table_key_columns': table_key_columns if primary_key_location == 'TABLE' else []
    }

    pipeline_handler = pipeline_handler_creator.create()
    oracle_cdc_origin = pipeline_handler.origin(attributes=oracle_cdc_attributes)
    snowflake_destination = pipeline_handler.destination(attributes=snowflake_attributes)

    oracle_cdc_origin >> snowflake_destination

    database_sql_connection_manager.insert(oracle_table, test_data)
    pipeline_handler.build(database, snowflake).run_until(count=len(test_data))

    oracle_data = database_sql_connection_manager.select_from_table(oracle_table)
    snowflake_data = snowflake_sql_connection_manager.select_from_table(snowflake_table)

    assert oracle_data == snowflake_data


@pytest.mark.parametrize('staging_location', INTERNAL_STAGING_LOCATION)
@pytest.mark.parametrize('cdc_test_case', CDC_TEST_CASES,
                         ids=['-'.join([row[OPERATION] for row in case.get(CDC_OPERATIONS)]) for case in CDC_TEST_CASES])
def test_cdc_merge_operations(sdc_builder, sdc_executor, database, snowflake, cleanup, database_sql_connection_manager,
                              snowflake_sql_connection_manager, snowflake_stage_name, oracle_stage_name,
                              pipeline_handler_creator, record_merger, staging_location, cdc_test_case):
    """
        Assert different merge operations behave as expected. This test also addresses Primary Key values updates.
    """
    columns = tuple(
        {Column(column, Integer, primary_key=column in cdc_test_case.get(PRIMARY_KEY_COLUMNS)) for column in
         CDC_DATABASE_COLUMNS})
    oracle_columns = deepcopy(columns)
    snowflake_columns = deepcopy(columns)

    oracle_table = database_sql_connection_manager.create_table(columns=oracle_columns)
    snowflake_table = snowflake_sql_connection_manager.create_table(columns=snowflake_columns)
    snowflake_stage = create_stage(snowflake, staging_location)

    oracle_cdc_attributes = {
        **DefaultConnectionParameters(database)  # Default parameters to connect to the DB
          | DefaultTableParameters(oracle_table.name)  # Default parameters to include a specific table
          | DefaultStartParameters(database)  # Default parameters to start from the current SCN
    }

    snowflake_attributes = {
        'stage_location': staging_location,
        'table': snowflake_table.name,
        'snowflake_stage_name': snowflake_stage,
        'processing_cdc_data': True
    }

    cdc_pipeline_handler = pipeline_handler_creator.create()
    oracle_cdc_origin = cdc_pipeline_handler.origin(attributes=oracle_cdc_attributes)
    snowflake_destination = cdc_pipeline_handler.destination(attributes=snowflake_attributes)

    oracle_cdc_origin >> snowflake_destination

    records_count = record_merger.merge_records([oracle_table])

    cdc_pipeline_handler.build(database, snowflake).run_until(count=records_count)
    oracle_data = database_sql_connection_manager.select_from_table(oracle_table)
    snowflake_data = snowflake_sql_connection_manager.select_from_table(snowflake_table)

    assert oracle_data == snowflake_data
