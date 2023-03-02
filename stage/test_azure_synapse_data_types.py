# Copyright 2023 StreamSets Inc.
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

import logging
import pytest
import string

from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string
from .utils.utils_azure_synapse import \
    SDC_COLUMN_TYPES,\
    STAGE_NAME,\
    get_columns,\
    get_data_warehouse_types_pipeline,\
    delete_table,\
    stop_pipeline

pytestmark = sdc_min_version('5.5.0')

logger = logging.getLogger(__name__)


@azure('synapse')
@pytest.mark.parametrize('is_cdc', [True, False])
def test_synapse_destination_data_types(sdc_builder, sdc_executor, azure, is_cdc):
    """
    Generate a record with different sdc data types and check they are properly mapped to the synapse types,
    as defined in:
    https://docs.streamsets.com/portal/datacollector/latest/help/datacollector/UserGuide/Destinations/AzureSynapse.html

    Synapse creates intermediate data when performing merge operations, which can lead to errors
    depending on the datatype size.

    The pipelines look like:
    dev_data_source >> azure_synapse_destination
    """
    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'

    drift_data_warehouse_pipeline = get_data_warehouse_types_pipeline(
        sdc_builder,
        azure,
        stage_file_prefix,
        destination_table_name,
        is_cdc=is_cdc
    )

    sdc_executor.add_pipeline(drift_data_warehouse_pipeline)
    engine = azure.synapse.engine
    try:
        # Run regular pipeline
        drift_pipeline_run_command = sdc_executor.start_pipeline(drift_data_warehouse_pipeline)
        drift_pipeline_run_command.wait_for_pipeline_output_records_count(2)
        sdc_executor.stop_pipeline(drift_data_warehouse_pipeline)

        # Retrieve table and columns
        column_info = get_columns(engine, destination_table_name)

        # Assert columns have been created with the expected data types
        assert column_info == [(row['name'], row['type']) for row in sorted(SDC_COLUMN_TYPES, key=lambda row: row['name'])]

    finally:
        try:
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, drift_data_warehouse_pipeline)
        except Exception as ex:
            logger.error(ex)


@azure('synapse')
def test_synapse_destination_propagate_numeric_values(sdc_builder, sdc_executor, azure):
    """
    Similar to test above, but making sure columns created with Replicate Decimal Columns
    have the columns properly created. We create them NUMERIC, which is the same as DECIMAL in Synapse.

    The pipelines look like:
    dev_data_source >> azure_synapse_destination
    """
    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'

    builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=1, delay_between_batches=10)

    dev_data_generator.fields_to_generate = [{
        'field': 'SDC_DECIMAL',
        'precision': 10,
        'scale': 2,
        'type': 'DECIMAL'
    }]

    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        auto_create_table=True,
        table=destination_table_name,
        purge_stage_file_after_loading=True,
        stage_file_prefix=stage_file_prefix,
        propagate_numeric_precision_and_scale=True
    )

    dev_data_generator >> azure_synapse_destination

    datalake_dest_pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(datalake_dest_pipeline)
    engine = azure.synapse.engine
    try:
        # Run regular pipeline
        drift_pipeline_run_command = sdc_executor.start_pipeline(datalake_dest_pipeline)
        drift_pipeline_run_command.wait_for_pipeline_output_records_count(2)
        sdc_executor.stop_pipeline(datalake_dest_pipeline)

        # Retrieve table and columns
        logger.info('Getting table metadata with name = %s...', destination_table_name)
        query = f"SELECT COLUMN_NAME, DATA_TYPE,  NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS " \
                f"WHERE TABLE_NAME = '{destination_table_name}' " \
                f"ORDER BY ORDINAL_POSITION;"
        result = engine.execute(query)
        column_info = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        # Assert columns have been created with the expected data types, precision and scale
        assert column_info == [('SDC_DECIMAL', 'numeric', 10, 2)]
    finally:
        try:
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, datalake_dest_pipeline)
        except Exception as ex:
            logger.error(ex)
