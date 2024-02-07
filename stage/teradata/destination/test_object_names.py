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
import string

import pytest

from stage.utils.common import cleanup
from . import (
    pytestmark,
    stage_name,
    teradata_manager,
    TROUBLESOME_TABLE_NAMES,
    TROUBLESOME_COLUMN_NAMES,
)
from stage.utils.data_loading_stages import (
    SIMPLE_ROWS,
    data_loading_pipeline_handler_creator,
    wildcard_field_name_simple_rows
)


@pytest.mark.parametrize('staging_location', ['LOCAL', 'ADLS_GEN2'])
@pytest.mark.parametrize('table_name_category, table_name', TROUBLESOME_TABLE_NAMES,
                         ids=[i[0] for i in TROUBLESOME_TABLE_NAMES])
def test_table_names(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                     data_loading_pipeline_handler_creator, staging_location, table_name_category, table_name):
    """
        Ensure that we can auto create and upload data to tables with any given name
    """
    test_data = SIMPLE_ROWS
    table = teradata_manager.describe_table(table_name=table_name)
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
    table = teradata_manager.populate_table(table)
    data_from_database = teradata_manager.select_from_table(table)

    assert data_from_database == test_data


@pytest.mark.parametrize('staging_location', ['LOCAL', 'ADLS_GEN2'])
@pytest.mark.parametrize('column_name_category, column_name', TROUBLESOME_COLUMN_NAMES,
                         ids=[i[0] for i in TROUBLESOME_COLUMN_NAMES])
def test_column_names(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                      data_loading_pipeline_handler_creator, staging_location, column_name_category, column_name):
    """
        Ensure that we can auto create and upload data to tables with any given column names.
        Note that we lower-case column names, as Teradata can only have them lower-cased.
    """
    test_data = wildcard_field_name_simple_rows(column_name)
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
    table = teradata_manager.populate_table(table)
    data_from_database = teradata_manager.select_from_table(table, order_by_column='id')

    assert data_from_database == test_data
