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

import json
import logging
import pytest
import string

from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string
from ..utils.utils_azure_synapse import \
    DATA_TYPES,\
    ROWS_IN_DATABASE,\
    STAGE_NAME,\
    delete_table,\
    get_synapse_pipeline,\
    stop_pipeline

pytestmark = [azure('synapse'), sdc_min_version('5.5.0')]

logger = logging.getLogger(__name__)


@pytest.mark.parametrize('table_name', ['random', 'table', 'select', 'from', 'table*', 'ta$ble'])
def test_object_names(sdc_builder, sdc_executor, azure, table_name):
    """
    Test for Azure Synapse target stage. We do so by running a dev raw data source to
    Azure Synapse destination and then reading data from SQL Server database for assertion.
    Standard reserved word for domain object test.

    The pipeline looks like:
    dev_raw_data_source >> azure_synapse_destination
    """
    engine = azure.synapse.engine
    if table_name == 'random':
        destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    else:
        destination_table_name = table_name
        try:
            stmt = f'''
                IF OBJECT_ID('[{azure.synapse_database_schema}].[{destination_table_name}]') IS NOT NULL
                BEGIN
                    DROP TABLE [{azure.synapse_database_schema}].[{destination_table_name}]
                END
            '''
            azure.synapse.engine.execute(stmt)
        except Exception as ex:
            logger.error(ex)

    logger.info('destination_table_name = %s', destination_table_name)
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'

    synapse_pipeline = get_synapse_pipeline(
        sdc_builder,
        azure,
        ROWS_IN_DATABASE,
        stage_file_prefix,
        destination_table_name
    )
    sdc_executor.add_pipeline(synapse_pipeline)
    try:
        sdc_executor.start_pipeline(synapse_pipeline).wait_for_finished()

        stmt = f'select * from [{azure.synapse_database_schema}].[{destination_table_name}]'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        dict_records_from_database = [{'id': element[0], 'name': element[1]} for element in data_from_database]
        for record in dict_records_from_database:
            assert record in ROWS_IN_DATABASE
    finally:
        try:
            delete_table(engine, "[" + destination_table_name + "]", "[" + azure.synapse_database_schema + "]")
            stop_pipeline(sdc_executor, synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


@pytest.mark.parametrize('number_of_threads', [1, 5])
def test_multiple_batches(sdc_builder, sdc_executor, azure, number_of_threads):
    """
    Test for Azure Synapse target stage multiple batches.
    We do so by creating 1000 rows x 100 columns using Dev Raw Generator.
    This is standard multiple batches test.

    dev_raw_generator >> azure_synapse_destination
    dev_raw_generator >> wiretap
    """

    engine = azure.synapse.engine
    total_records = 1000
    batch_size = total_records // 5
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = f'{src_table_prefix}_{get_random_string()}'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(
        batch_size=batch_size,
        number_of_threads=number_of_threads,
        fields_to_generate=[{
            'field': f'text{i}',
            'type': 'STRING'
        } for i in range(5)]
    )
    azure_synapse_destination = pipeline_builder.add_stage(name=STAGE_NAME)
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'
    azure_synapse_destination.set_attributes(
        auto_create_table=True,
        table=table_name,
        stage_file_prefix=stage_file_prefix
    )
    wiretap = pipeline_builder.add_wiretap()
    dev_data_generator >> [wiretap.destination, azure_synapse_destination]
    pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(total_records)
        sdc_executor.stop_pipeline(pipeline)

        stmt = f'select * from [{azure.synapse_database_schema}].[{table_name}]'
        result = azure.synapse.engine.execute(stmt)
        records = sorted(
            [sorted(tuple(str(field) for field in record.field.values())) for record in wiretap.output_records],
            key=lambda record: record[0]
        )
        records_from_database = sorted(
            [sorted(tuple(str(field) for field in record)) for record in result.fetchall()],
            key=lambda record: record[0]
        )

        assert records == records_from_database
    finally:
        logger.info('Dropping table %s in Synapse...', table_name)
        try:
            stop_pipeline(sdc_executor, pipeline)
            delete_table(engine, table_name, azure.synapse_database_schema)
        except Exception as ex:
            logger.error(ex)


@pytest.mark.parametrize(
    'input, converter_type, database_type, expected',
    DATA_TYPES, ids=[f"{i[1]}-{i[2]}" for i in DATA_TYPES]
)
def test_data_types(sdc_builder, sdc_executor, input, converter_type, database_type, expected, azure, keep_data):
    """
    Several data types got fixed in 1.3 (AZUREDW-127), so skipping tests for previous versions.
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 20)}'
    engine = azure.synapse.engine

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.stop_after_first_batch = True
    origin.raw_data = json.dumps({"VALUE": input })

    converter = builder.add_stage('Field Type Converter')
    converter.conversion_method = 'BY_FIELD'
    converter.field_type_converter_configs = [{
        'fields': ['/VALUE'],
        'targetType': converter_type,
        'dataLocale': 'en,US',
        'dateFormat': 'YYYY_MM_DD_HH_MM_SS',
        'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
        'scale': 2
    }]
    if converter_type == 'TIME':
        converter.field_type_converter_configs = [{
            'fields': ['/VALUE'],
            'targetType': converter_type,
            'dataLocale': 'en,US',
            'dateFormat': 'OTHER',
            'otherDateFormat': 'HH:MM:SS',
            'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
            'scale': 2
        }]

    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'

    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        auto_create_table=True,
        table=table_name,
        purge_stage_file_after_loading=True,
        stage_file_prefix=stage_file_prefix,
        ignore_missing_fields=False,
        ignore_fields_with_invalid_types=False
    )

    origin >> converter >> azure_synapse_destination
    pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table
        logger.info(f'Create Table {azure.synapse_database_schema}.{table_name}')
        engine.execute(f"""
            CREATE TABLE {azure.synapse_database_schema}.{table_name} (
                "VALUE" {database_type} NULL
            )
        """)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        logger.info("Create Table Result")
        rs = engine.execute(f'select VALUE from {azure.synapse_database_schema}.{table_name}')
        data_from_database = rs.fetchall()
        rs.close()

        assert len(data_from_database) == 1
        assert data_from_database[0][0] == expected

        # And we will also assert that the type is correct
        query = f"SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS" \
                f" WHERE TABLE_NAME = '{table_name}' ORDER BY ORDINAL_POSITION;"
        result = engine.execute(query)
        column_info = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()
        datatype_from_db = column_info[0][0]
        expected_datatype = database_type.split("(")[0]

        assert datatype_from_db == expected_datatype
    finally:
        if not keep_data:
            try:
                delete_table(engine, table_name, azure.synapse_database_schema)
                stop_pipeline(sdc_executor, pipeline)
            except Exception as ex:
                logger.error(ex)
