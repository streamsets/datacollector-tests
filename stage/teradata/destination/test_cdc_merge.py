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

from stage.utils.common import cleanup
from . import (
    pytestmark,
    stage_name,
    teradata_manager,
    ALL_STAGING_LOCATIONS,
    DEFAULT_EXTERNAL_STAGING_LOCATION
)
from stage.utils.data_loading_stages import (
    SIMPLE_ROWS,
    CDC_DATABASE_COLUMNS,
    CDC_TEST_CASES,
    CDC_MULTI_TABLE_TEST_CASES,
    PRIMARY_KEY_COLUMNS,
    TABLE,
    OPERATION,
    SETUP_OPERATIONS,
    CDC_OPERATIONS,
    EXPECTED_RESULT,
    data_loading_pipeline_handler_creator,
    data_loading_cdc_record_creator
)
from sqlalchemy import (
    Table,
    MetaData,
    Column,
    String,
    Integer
)


@pytest.mark.parametrize('staging_location', ALL_STAGING_LOCATIONS)
@pytest.mark.parametrize('primary_key_location', ['HEADER', 'TABLE'])
def test_cdc_merge(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                   data_loading_pipeline_handler_creator, staging_location, primary_key_location):
    """
        Assert that some rows are properly loaded when the table is created by the pipeline
        and using Merge CDC Data. Important to check if CDC op column is properly handled.
    """
    test_data = SIMPLE_ROWS
    primary_key_columns = ['id']
    table = teradata_manager.create_table()
    authorization = teradata_manager.create_authorization()

    table_key_columns = [{
        'keyColumns': primary_key_columns,
        'table': table.name
    }]

    teradata_attributes = {
        'staging_location': staging_location,
        'table': table.name,
        'authorization_name': authorization.name,
        'merge_cdc_data': True,
        'primary_key_location': primary_key_location,
        'table_key_columns': table_key_columns if primary_key_location == 'TABLE' else []
    }

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = pipeline_handler.dev_raw_data_source_origin(test_data)
    cdc_evaluator = pipeline_handler.insert_operation_evaluator(primary_key_columns)
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> cdc_evaluator >> teradata_destination

    pipeline_handler.build(teradata).run()
    data_from_database = teradata_manager.select_from_table(table)
    assert data_from_database == test_data


@pytest.mark.parametrize('staging_location', DEFAULT_EXTERNAL_STAGING_LOCATION)
@pytest.mark.parametrize('primary_key_location', ['HEADER', 'TABLE'])
@pytest.mark.parametrize('cdc_test_case', CDC_TEST_CASES,
                         ids=['-'.join([row[OPERATION] for row in case.get(CDC_OPERATIONS)]) for case in CDC_TEST_CASES])
def test_cdc_merge_operations(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                              data_loading_pipeline_handler_creator, data_loading_cdc_record_creator, staging_location,
                              primary_key_location, cdc_test_case):
    """
        Assert different merge operations behave as expected. This test also addresses Primary Key values updates.
        It uses 2 pipelines, one to set up the db and pipeline state, and the second one to perform the CDC operations.
    """
    columns = tuple(
        {Column(column, Integer, primary_key=column in cdc_test_case.get(PRIMARY_KEY_COLUMNS))
         for column in CDC_DATABASE_COLUMNS})
    table = teradata_manager.create_table(teradata_manager.describe_table(columns=columns))
    authorization = teradata_manager.create_authorization()

    table_key_columns = [{
        'keyColumns': cdc_test_case.get(PRIMARY_KEY_COLUMNS),
        'table': table.name
    }]

    teradata_attributes = {
        'staging_location': staging_location,
        'table': table.name,
        'authorization_name': authorization.name,
        'merge_cdc_data': True,
        'primary_key_location': primary_key_location,
        'table_key_columns': table_key_columns if primary_key_location == 'TABLE' else []
    }

    cdc_header_expressions = data_loading_cdc_record_creator.cdc_header_expressions()
    cdc_field_remover_fields = data_loading_cdc_record_creator.cdc_field_remover_fields()
    setup_records = data_loading_cdc_record_creator.generate_setup_records()
    cdc_records = data_loading_cdc_record_creator.generate_cdc_records()

    if len(setup_records) > 0:
        setup_pipeline_handler = data_loading_pipeline_handler_creator.create()
        dev_raw_data_source = setup_pipeline_handler.dev_raw_data_source_origin(setup_records)
        expression_evaluator = setup_pipeline_handler.expression_evaluator(cdc_header_expressions)
        field_remover = setup_pipeline_handler.field_remover(cdc_field_remover_fields)
        teradata_destination = setup_pipeline_handler.destination(attributes=teradata_attributes)

        dev_raw_data_source >> expression_evaluator >> field_remover >> teradata_destination

        setup_pipeline_handler.build(teradata).run()

    cdc_pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = cdc_pipeline_handler.dev_raw_data_source_origin(cdc_records)
    expression_evaluator = cdc_pipeline_handler.expression_evaluator(cdc_header_expressions)
    field_remover = cdc_pipeline_handler.field_remover(cdc_field_remover_fields)
    teradata_destination = cdc_pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> expression_evaluator >> field_remover >> teradata_destination

    cdc_pipeline_handler.build(teradata).run()

    data_from_database = teradata_manager.select_from_table(table)
    assert data_from_database == cdc_test_case.get(EXPECTED_RESULT)


@pytest.mark.parametrize('staging_location', DEFAULT_EXTERNAL_STAGING_LOCATION)
@pytest.mark.parametrize('primary_key_location', ['HEADER', 'TABLE'])
@pytest.mark.parametrize('cdc_test_case', CDC_MULTI_TABLE_TEST_CASES,
                         ids=['-'.join([row[OPERATION] for row in case.get(CDC_OPERATIONS)]) for case in CDC_MULTI_TABLE_TEST_CASES])
def test_cdc_merge_multiple_tables(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                                   data_loading_pipeline_handler_creator, data_loading_cdc_record_creator,
                                   staging_location, primary_key_location, cdc_test_case):
    """
        Assert different merge operations behave as expected with different target tables.
    """
    def set_table_per_record(records, table_names):
        for record in records:
            record[TABLE] = table_names[record[TABLE]]

    columns_1 = tuple(
        {Column(column, Integer, primary_key=column in cdc_test_case.get(PRIMARY_KEY_COLUMNS))
         for column in CDC_DATABASE_COLUMNS})
    table_1 = teradata_manager.create_table(teradata_manager.describe_table(columns=columns_1))
    columns_2 = tuple(
        {Column(column, Integer, primary_key=column in cdc_test_case.get(PRIMARY_KEY_COLUMNS))
         for column in CDC_DATABASE_COLUMNS})
    table_2 = teradata_manager.create_table(teradata_manager.describe_table(columns=columns_2))
    tables = [table_1, table_2]
    table_names = [table_1.name, table_2.name]
    authorization = teradata_manager.create_authorization()

    table_key_columns = [{
        "keyColumns": cdc_test_case.get(PRIMARY_KEY_COLUMNS),
        "table": table_name
    } for table_name in table_names]

    teradata_attributes = {
        'staging_location': staging_location,
        'table': "${record:attribute('TABLE')}",
        'authorization_name': authorization.name,
        'merge_cdc_data': True,
        'table_key_columns': table_key_columns if primary_key_location == 'TABLE' else []
    }

    cdc_header_expressions = data_loading_cdc_record_creator.cdc_header_expressions()
    cdc_field_remover_fields = data_loading_cdc_record_creator.cdc_field_remover_fields()
    setup_records = data_loading_cdc_record_creator.generate_setup_records()
    cdc_records = data_loading_cdc_record_creator.generate_cdc_records()

    set_table_per_record(setup_records, table_names)
    set_table_per_record(cdc_records, table_names)

    if len(setup_records) > 0:
        setup_pipeline_handler = data_loading_pipeline_handler_creator.create()
        dev_raw_data_source = setup_pipeline_handler.dev_raw_data_source_origin(setup_records)
        expression_evaluator = setup_pipeline_handler.expression_evaluator(cdc_header_expressions)
        field_remover = setup_pipeline_handler.field_remover(cdc_field_remover_fields)
        teradata_destination = setup_pipeline_handler.destination(attributes=teradata_attributes)

        dev_raw_data_source >> expression_evaluator >> field_remover >> teradata_destination

        setup_pipeline_handler.build(teradata).run()

    cdc_pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = cdc_pipeline_handler.dev_raw_data_source_origin(cdc_records)
    expression_evaluator = cdc_pipeline_handler.expression_evaluator(cdc_header_expressions)
    field_remover = cdc_pipeline_handler.field_remover(cdc_field_remover_fields)
    teradata_destination = cdc_pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> expression_evaluator >> field_remover >> teradata_destination

    cdc_pipeline_handler.build(teradata).run()

    for table, expected_result_per_table in zip(tables, cdc_test_case.get(EXPECTED_RESULT).values()):
        data_from_database = teradata_manager.select_from_table(table)
        assert data_from_database == expected_result_per_table
