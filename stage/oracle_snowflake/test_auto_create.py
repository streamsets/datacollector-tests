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
    DefaultStartParameters
)
from stage.utils.data_loading_stages import (
    CDC_DATABASE_COLUMNS,
    CDC_MULTI_TABLE_TEST_CASES,
    PRIMARY_KEY_COLUMNS,
    OPERATION,
    CDC_OPERATIONS,
)
from sqlalchemy import (
    Column,
    Integer
)


@pytest.mark.parametrize('staging_location', INTERNAL_STAGING_LOCATION)
@pytest.mark.parametrize('cdc_test_case', CDC_MULTI_TABLE_TEST_CASES,
                         ids=['-'.join([row[OPERATION] for row in case.get(CDC_OPERATIONS)]) for case in CDC_MULTI_TABLE_TEST_CASES])
def test_cdc_merge_auto_create_multiple_tables(sdc_builder, sdc_executor, database, snowflake, cleanup,
                                               database_sql_connection_manager, snowflake_sql_connection_manager,
                                               snowflake_stage_name, oracle_stage_name, pipeline_handler_creator,
                                               record_merger, staging_location, cdc_test_case):
    """
        Assert different merge operations behave as expected with different target tables.
        We use the EL oracle.cdc.table to create tables as needed in Snowflake.
    """
    number_of_tables = 2
    columns = tuple(
        {Column(column, Integer, primary_key=column in cdc_test_case.get(PRIMARY_KEY_COLUMNS)) for column in
         CDC_DATABASE_COLUMNS})
    oracle_columns = [deepcopy(columns) for _ in range(0, number_of_tables)]

    oracle_tables = [database_sql_connection_manager.create_table(columns=columns) for columns in oracle_columns]
    snowflake_stage = create_stage(snowflake, staging_location)

    oracle_cdc_attributes = {
        **DefaultConnectionParameters(database)  # Default parameters to connect to the DB
          | DefaultStartParameters(database)  # Default parameters to start from the current SCN
          | {"tables_filter": [{"tablesInclusionPattern": table.name} for table in oracle_tables]}
    }

    snowflake_attributes = {
        'stage_location': staging_location,
        'table': "${record:attribute('oracle.cdc.table')}",
        'snowflake_stage_name': snowflake_stage,
        'processing_cdc_data': True,
        'table_auto_create': True,
        'data_drift_enabled': True
    }

    cdc_pipeline_handler = pipeline_handler_creator.create()
    oracle_cdc_origin = cdc_pipeline_handler.origin(attributes=oracle_cdc_attributes)
    snowflake_destination = cdc_pipeline_handler.destination(attributes=snowflake_attributes)

    oracle_cdc_origin >> snowflake_destination

    records_count = record_merger.merge_records(oracle_tables)
    cdc_pipeline_handler.build(database, snowflake).run_until(count=records_count)

    for oracle_table in oracle_tables:
        oracle_data = database_sql_connection_manager.select_from_table(oracle_table)
        snowflake_table = snowflake_sql_connection_manager.describe_table(table_name=oracle_table.name)
        snowflake_table = snowflake_sql_connection_manager.populate_table(table=snowflake_table)
        snowflake_data = snowflake_sql_connection_manager.select_from_table(snowflake_table)

        assert oracle_data == snowflake_data
