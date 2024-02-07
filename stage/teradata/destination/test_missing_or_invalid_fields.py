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
    teradata_manager
)
from stage.utils.data_loading_stages import (
    wildcard_field_simple_rows,
    data_loading_pipeline_handler_creator
)
from sqlalchemy import (
    Table,
    MetaData,
    Column,
    String,
    Integer
)


@pytest.mark.parametrize('staging_location, numeric_default', [
    ['LOCAL', None],
    ['ADLS_GEN2', -1]  # for some reason external tables won't work with partial rows, so we change the missing field
])
@pytest.mark.parametrize('ignore_missing_fields', [True, False])
def test_ignore_missing_fields(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                               data_loading_pipeline_handler_creator, staging_location, numeric_default,
                               ignore_missing_fields):
    """
        Assert that some rows are properly loaded even if one column is not present
    """
    test_data = wildcard_field_simple_rows({})
    table = teradata_manager.describe_table()
    authorization = teradata_manager.create_authorization()

    teradata_attributes = {
        'staging_location': staging_location,
        'table': table.name,
        'authorization_name': authorization.name,
        'enable_data_drift': True,
        'auto_create_table': True,
        'numeric_types_default': None if numeric_default is None else str(numeric_default),
        'ignore_missing_fields': ignore_missing_fields
    }

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = pipeline_handler.dev_raw_data_source_origin(test_data)
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> teradata_destination

    pipeline_handler.build(teradata).run()
    data_from_database = teradata_manager.select_from_table(table)

    if ignore_missing_fields:
        # we get the 3 rows, but with the first one having None instead
        assert data_from_database == wildcard_field_simple_rows({'id': numeric_default})
    else:
        # we get 2 rows, as the 1st one went to error
        assert data_from_database == test_data[1:]


@pytest.mark.parametrize('staging_location, numeric_default', [
    ['LOCAL', None],
    ['ADLS_GEN2', -1]  # for some reason external tables won't work with partial rows, so we change the invalid field
])
@pytest.mark.parametrize('ignore_fields_with_invalid_types', [True, False])
def test_ignore_fields_with_invalid_types(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                                          data_loading_pipeline_handler_creator, staging_location, numeric_default,
                                          ignore_fields_with_invalid_types):
    """
        Assert that some rows are properly loaded even if one column contains invalid types
    """
    test_data = wildcard_field_simple_rows({'id': 'not_an_integer'})
    columns = tuple(({Column('name', String(32), primary_key=True), Column('id', Integer)}))
    table = teradata_manager.create_table(teradata_manager.describe_table(columns=columns))
    authorization = teradata_manager.create_authorization()

    teradata_attributes = {
        'staging_location': staging_location,
        'table': table.name,
        'authorization_name': authorization.name,
        'enable_data_drift': True,
        'auto_create_table': True,
        'numeric_types_default': None if numeric_default is None else str(numeric_default),
        'ignore_fields_with_invalid_types': ignore_fields_with_invalid_types
    }

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = pipeline_handler.dev_raw_data_source_origin(test_data)
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> teradata_destination

    pipeline_handler.build(teradata).run()
    data_from_database = teradata_manager.select_from_table(table)

    if ignore_fields_with_invalid_types:
        # we get the 3 rows, but with the first one having '' instead
        assert data_from_database == wildcard_field_simple_rows({'id': numeric_default})
    else:
        # we get 2 rows, as the 1st one went to error
        assert data_from_database == test_data[1:]
