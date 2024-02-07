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


@pytest.mark.parametrize('staging_location', LOCAL_STAGING_LOCATION)
def test_eval_table(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                    data_loading_pipeline_handler_creator, staging_location):
    """
        Assert that some rows are properly loaded when the table is created by the pipeline
        using EL expressions in the table field.
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
