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
    DEFAULT_EXTERNAL_STAGING_LOCATION
)
from stage.utils.data_loading_stages import (
    OnRecordErrorStatus,
    MISSING_FIELD_ROWS,
    data_loading_pipeline_handler_creator,
    data_loading_on_record_error_handler
)


@pytest.mark.parametrize('staging_location', DEFAULT_EXTERNAL_STAGING_LOCATION)
@pytest.mark.parametrize('on_record_error, on_record_error_status, test_data, expected_data', [
    (
            'STOP_PIPELINE',
            OnRecordErrorStatus("DATA_LOADING_18 - Missing field 'id'", 0, 0, 0),
            MISSING_FIELD_ROWS,
            []  # nothing in the db, pipeline gets stopped on the spot
    ),
    (
            'DISCARD',
            OnRecordErrorStatus('', 3, 3, 0),
            MISSING_FIELD_ROWS,
            MISSING_FIELD_ROWS[1:]  # the incorrect row doesn't get to the database
    ),
    (
            'TO_ERROR',
            OnRecordErrorStatus('', 3, 2, 1),
            MISSING_FIELD_ROWS,
            MISSING_FIELD_ROWS[1:]  # the incorrect row doesn't get to the database
    )
], ids=['STOP_PIPELINE', 'DISCARD', 'TO_ERROR'])
def test_on_record_error(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                         data_loading_pipeline_handler_creator, data_loading_on_record_error_handler,
                         staging_location, on_record_error_status, test_data, on_record_error, expected_data):
    """
        Assert that some error behaviour works as expected. We force an error by sending a partial row.
    """
    table = teradata_manager.create_table()
    authorization = teradata_manager.create_authorization()

    teradata_attributes = {
        'staging_location': staging_location,
        'table': table.name,
        'authorization_name': authorization.name,
        'ignore_missing_fields': False,
        'on_record_error': on_record_error
    }

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = pipeline_handler.dev_raw_data_source_origin(test_data)
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> teradata_destination

    data_loading_on_record_error_handler.run_and_handle_on_record_error(pipeline_handler.build(teradata))
    data_from_database = teradata_manager.select_from_table(table)

    assert data_from_database == expected_data


@pytest.mark.parametrize('staging_location', DEFAULT_EXTERNAL_STAGING_LOCATION)
@pytest.mark.parametrize('on_record_error, on_record_error_status, test_data, expected_data', [
    (
            'STOP_PIPELINE',
            OnRecordErrorStatus({'5.11.0': "DATA_LOADING_23 - Record is missing the 'id' key field",
                                 '6.0.0': "DATA_LOADING_23 - The record is missing the 'id' field, which is a primary key"},
                                0, 0, 0),
            MISSING_FIELD_ROWS,
            []  # nothing in the db, pipeline gets stopped on the spot
    ),
    (
            'DISCARD',
            OnRecordErrorStatus('', 3, 3, 0),
            MISSING_FIELD_ROWS,
            MISSING_FIELD_ROWS[1:]  # the incorrect row doesn't get to the database
    ),
    (
            'TO_ERROR',
            OnRecordErrorStatus('', 3, 2, 1),
            MISSING_FIELD_ROWS,
            MISSING_FIELD_ROWS[1:]  # the incorrect row doesn't get to the database
    )
], ids=['STOP_PIPELINE', 'DISCARD', 'TO_ERROR'])
def test_on_record_error_with_merge(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                                    data_loading_pipeline_handler_creator, staging_location,
                                    data_loading_on_record_error_handler, on_record_error, on_record_error_status,
                                    test_data, expected_data):
    """
        Assert that some error behaviour works as expected if MERGE is enabled.
        We force an error by sending a partial row.
    """
    primary_key_columns = ['id']
    table = teradata_manager.create_table()
    authorization = teradata_manager.create_authorization()

    teradata_attributes = {
        'staging_location': staging_location,
        'table': table.name,
        'authorization_name': authorization.name,
        'ignore_missing_fields': False,
        'merge_cdc_data': True,
        'on_record_error': on_record_error
    }

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_raw_data_source = pipeline_handler.dev_raw_data_source_origin(test_data)
    cdc_evaluator = pipeline_handler.insert_operation_evaluator(primary_key_columns)
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    dev_raw_data_source >> cdc_evaluator >> teradata_destination

    data_loading_on_record_error_handler.run_and_handle_on_record_error(pipeline_handler.build(teradata))
    data_from_database = teradata_manager.select_from_table(table)

    assert data_from_database == expected_data
