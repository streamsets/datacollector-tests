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
    LOCAL_STAGING_LOCATION
)
from stage.utils.data_loading_stages import (
    SIMPLE_ROWS,
    data_loading_pipeline_handler_creator
)
from sqlalchemy import (
    Table,
    MetaData,
    Column,
    String,
    Integer
)


@pytest.mark.parametrize('staging_location', LOCAL_STAGING_LOCATION)
def test_data_drift_table_auto_create(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                                      data_loading_pipeline_handler_creator, staging_location):
    """
        Assert table is properly created by the stage.
    """
    test_data = SIMPLE_ROWS
    table = teradata_manager.describe_table()
    authorization = teradata_manager.create_authorization()

    teradata_attributes = {
        'staging_location': staging_location,
        'table': table.name,
        'authorization_name': authorization.name,
        'auto_create_table': True,
        'enable_data_drift': True
    }

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = pipeline_handler.dev_raw_data_source_origin(test_data)
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> teradata_destination

    pipeline_handler.build(teradata).run()
    data_from_database = teradata_manager.select_from_table(table)

    assert data_from_database == test_data


@pytest.mark.parametrize('staging_location', LOCAL_STAGING_LOCATION)
def test_data_drift_create_columns(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                                   data_loading_pipeline_handler_creator, staging_location):
    """
        Assert new columns are properly created by the stage.
    """
    test_data = SIMPLE_ROWS
    columns = tuple({Column('id', Integer)})  # we do not create 'name' so it gets created by the stage
    table = teradata_manager.create_table(teradata_manager.describe_table(columns=columns))
    table = teradata_manager.describe_table(table.name)  # the final description will hold all the columns
    authorization = teradata_manager.create_authorization()

    teradata_attributes = {
        'staging_location': staging_location,
        'table': table.name,
        'authorization_name': authorization.name,
        'auto_create_table': True,
        'enable_data_drift': True
    }

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = pipeline_handler.dev_raw_data_source_origin(test_data)
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> teradata_destination

    pipeline_handler.build(teradata).run()
    data_from_database = teradata_manager.select_from_table(table)

    assert data_from_database == test_data


@pytest.mark.parametrize('staging_location', LOCAL_STAGING_LOCATION)
def test_data_drift_table_auto_create_with_primary_key(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager,
                                                       stage_name, data_loading_pipeline_handler_creator,
                                                       staging_location):
    """
        Assert new columns with the primary key constraint are properly created by the stage.
    """
    test_data = SIMPLE_ROWS
    table = teradata_manager.describe_table()
    authorization = teradata_manager.create_authorization()

    teradata_attributes = {
        'staging_location': staging_location,
        'table': table.name,
        'authorization_name': authorization.name,
        'auto_create_table': True,
        'enable_data_drift': True
    }

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = pipeline_handler.dev_raw_data_source_origin(test_data)
    # we force the primary key header to be 'id', so it is used even if no cdc operation is performed
    cdc_evaluator = pipeline_handler.insert_operation_evaluator(['id'])
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> cdc_evaluator >> teradata_destination

    pipeline_handler.build(teradata).run()
    data_from_database = teradata_manager.select_from_table(table)

    assert data_from_database == test_data


@pytest.mark.parametrize('staging_location', LOCAL_STAGING_LOCATION)
def test_data_drift_create_columns_as_string(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                                             data_loading_pipeline_handler_creator, staging_location):
    """
        Assert new columns created as string are properly created by the stage as a String.
    """
    test_data = SIMPLE_ROWS
    expected_data = [{k: str(v) for k, v in row.items()} for row in SIMPLE_ROWS]
    table = teradata_manager.describe_table()
    authorization = teradata_manager.create_authorization()

    teradata_attributes = {
        'staging_location': staging_location,
        'table': table.name,
        'authorization_name': authorization.name,
        'auto_create_table': True,
        'enable_data_drift': True,
        'create_new_columns_as_string': True
    }

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = pipeline_handler.dev_raw_data_source_origin(test_data)
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> teradata_destination

    pipeline_handler.build(teradata).run()
    data_from_database = teradata_manager.select_from_table(table)

    assert data_from_database == expected_data


@pytest.mark.parametrize('staging_location', LOCAL_STAGING_LOCATION)
def test_data_drift_table_auto_create_with_el(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager,
                                              stage_name, data_loading_pipeline_handler_creator, staging_location):
    """
        Assert new tables coming from EL expressions are properly created by the stage.
    """
    test_data = SIMPLE_ROWS
    table = teradata_manager.describe_table()
    authorization = teradata_manager.create_authorization()

    teradata_attributes = {
        'staging_location': staging_location,
        'table': "${record:attribute('TABLE')}",
        'authorization_name': authorization.name,
        'auto_create_table': True,
        'enable_data_drift': True
    }

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = pipeline_handler.dev_raw_data_source_origin(test_data)
    expression_evaluator = pipeline_handler.expression_evaluator(header_attribute_expressions=[
        {'attributeToSet': 'TABLE',
         'headerAttributeExpression': table.name}
    ])
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> expression_evaluator >> teradata_destination

    pipeline_handler.build(teradata).run()
    data_from_database = teradata_manager.select_from_table(table)

    assert data_from_database == test_data
