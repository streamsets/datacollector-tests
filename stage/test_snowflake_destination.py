# Copyright 2022 StreamSets Inc.
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
import os
import random
import string
import tempfile
import time

import pytest
import sqlalchemy
from sqlalchemy.sql import text
from streamsets.sdk.sdc_api import StartError
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import aws, snowflake, sdc_enterprise_lib_min_version, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# from CommonDatabaseHeader.java
PRIMARY_KEY_COLUMN_OLD_VALUE = 'jdbc.primaryKey.before'
PRIMARY_KEY_COLUMN_NEW_VALUE = 'jdbc.primaryKey.after'
PRIMARY_KEY_SPECIFICATION = 'jdbc.primaryKeySpecification'

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Roger Federer'},
    {'id': 2, 'name': 'Rafael Nadal'},
    {'id': 3, 'name': 'Dominic Thiem'}
]

ROWS_IN_DATABASE_WITH_ERROR = [
    {'id': 1, 'name': 'Roger Federer'},
    {'id': 2, 'name': 'Rafael Nadal'},
    {'name': 'a'}, # Wihtout the 'id' column, the stage tries to add it with null value, and then we force snowflake to fail
    {'name': 'a'},
    {'id': 5, 'name': 'Pete Sampras'},
]

ROWS_IN_DATABASE_WITH_ERROR_ABORT_STATEMENT = [
]

ROWS_IN_DATABASE_WITH_ERROR_CONTINUE = [
    {'id': 1, 'name': 'Roger Federer'},
    {'id': 2, 'name': 'Rafael Nadal'},
    {'id': 5, 'name': 'Pete Sampras'},
]

SPACED_ROWS_IN_DATABASE = [
    {'id': 1, 'name': ' Roger Federer'},
    {'id': 2, 'name': ' Rafael Nadal'},
    {'id': 3, 'name': ' Dominic Thiem'}
]

DRIFT_ROWS_IN_DATABASE = [
    {'id': 4, 'name': 'Arthur Ashe', 'ranking': 1},
    {'id': 5, 'name': 'Ivan Lendl', 'ranking': 2},
    {'id': 6, 'name': 'Guillermo Vilas', 'ranking': 12}
]
DRIFT_ROWS_IN_DATABASE_WRONG_COLUMN = [
    {'id': 7, 'name': 'John', '\"wrongColumn\"{f1': 'abc'}
]
CDC_ROWS_IN_DATABASE = [
    {'OP': 1, 'ID': 1, 'NAME': 'Rogelio Federer'},
    {'OP': 1, 'ID': 2, 'NAME': 'Rafa Nadal'},
    {'OP': 1, 'ID': 3, 'NAME': 'Domi Thiem'}
]
CDC_ROWS_IN_DATABASE_COMPOSITE_KEY = [
    {'OP': 1, 'A': 1, 'B': 3, 'NAME': 'Rogelio Federer'},
    {'OP': 1, 'A': 2, 'B': 2, 'NAME': 'Rafa Nadal'},
    {'OP': 1, 'A': 3, 'B': 1, 'NAME': 'Domi Thiem'}
]

CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY = [
    {'TYPE': 'Hobbit', 'ID': 1, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 0'},
    {'TYPE': 'Fallohide', 'ID': 1, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 1'},
    {'TYPE': 'Fallohide', 'ID': 2, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 2'},
    {'TYPE': 'Hobbit - Fallohide', 'ID': 3, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 3'},
    {'TYPE': 'Hobbit, Fallohide', 'ID': 3, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 4'},
    {'TYPE': 'Hobbit, Fallohide', 'ID': 4, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 5'},
    {'TYPE': 'Hobbit, Fallohide', 'ID': 4, 'NAME': 'Bilbo', 'SURNAME': 'Baggins', 'ADDRESS': 'Bag End 6'},
]
CDC_PK_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY_HEADER = [
    {'sdc.operation.type': 1, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 1, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 1,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 1, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 1,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Fallohide'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 1, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 2,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Fallohide', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Fallohide'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 2, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 3,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Fallohide', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit - Fallohide'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 3, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 3,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit - Fallohide', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit, Fallohide'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 3, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 4,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit, Fallohide', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit, Fallohide'},

    {'sdc.operation.type': 3, f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 4, f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 4,
     f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit, Fallohide', f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit, Fallohide'},
]

# AWS S3 bucket in case of AWS or Azure blob storage container in case of Azure.
STORAGE_BUCKET_CONTAINER = 'snowflake'


@snowflake
@sdc_min_version('3.7.0')
@pytest.mark.parametrize('stage_location', ["INTERNAL", "AWS_S3", "AZURE", "GCS"])
def test_basic(sdc_builder, sdc_executor, snowflake, stage_location):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    We assert the data from the client to what has been ingested by the Snowflake pipeline.
    We test for the different staging areas available.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >> snowflake_destination
    """
    _run_test_basic(sdc_builder, sdc_executor, snowflake, stage_location)


@snowflake
@aws('s3', 'kms')
@sdc_min_version('3.7.0')
def test_basic_aws_sse_kms(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage using AWS SSE-KMS. Data is inserted into Snowflake using the
    pipeline. After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    We assert the data from the client to what has been ingested by the Snowflake pipeline.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >> snowflake_destination
    """
    _run_test_basic(sdc_builder, sdc_executor, snowflake, 'AWS_S3', sse_kms=True)


@snowflake
@sdc_min_version('3.7.0')
def test_basic_azure_sas_token(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage using Azure SAS Token. Data is inserted into Snowflake using the
    pipeline. After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    We assert the data from the client to what has been ingested by the Snowflake pipeline.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >> snowflake_destination
    """
    _run_test_basic(sdc_builder, sdc_executor, snowflake, 'AZURE', sas_token=True)


def _run_test_basic(sdc_builder, sdc_executor, snowflake, stage_location, sse_kms=False, sas_token=False):
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # Create a table and stage in Snowflake.
    table = snowflake.create_table(table_name.lower())
    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location=stage_location)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    raw_data = '\n'.join(json.dumps(row) for row in ROWS_IN_DATABASE)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(stage_location=stage_location,
                                         purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name)
    if sse_kms:
        # Use SSE with KMS (other necessary SSE-KMS configs set by snowflake environment)
        snowflake_destination.set_attributes(s3_encryption='KMS')
    if sas_token:
        # Use Azure SAS Token to authenticate
        snowflake_destination.set_attributes(azure_authentication='SAS_TOKEN')

    dev_raw_data_source >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == [(row['name'], row['id']) for row in ROWS_IN_DATABASE]
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
@pytest.mark.parametrize('data_drift_enabled', [True, False])
def test_basic_snowpipe(sdc_builder, sdc_executor, snowflake, data_drift_enabled):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    We assert the data from the client to what has been ingested by the Snowflake pipeline.
    Here we set the Snowpipe to ingest data. Note that when Snowpipe is enabled, Drift is not used.

    NOTE: When using GCS staging, snowpipe tests are most likely going to fail, due to limitations in Snowflake
    capabilities: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-ts.html#loads-from-google-cloud-storage-delayed-or-files-missed
    So we just skip them. But if there is the need of trying Snowpipe+gcs, they can eventually pass.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'
    pipe_name = f'STF_PIPE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Create a table, stage and pipe in Snowflake.
    table = snowflake.create_table(table_name.lower())
    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)
    snowflake.create_pipe(pipe_name, stage_name, table_name)

    # Build the pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    raw_data = '\n'.join(json.dumps(row) for row in ROWS_IN_DATABASE)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(pipe=pipe_name,
                                         purge_stage_file_after_ingesting=False,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         use_snowpipe=True,
                                         data_drift_enabled=data_drift_enabled)

    if Version(snowflake_destination.stage_version) == Version('8') and not data_drift_enabled:
        pytest.skip(f'Skip for version {snowflake_destination.stage_version}')

    dev_raw_data_source >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        def query_function():
            return engine.execute(table.select())

        data_from_database = wait_for_snowpipe_data_ingestion(query_function)

        assert len(data_from_database) > 0, "There should be some data in the database, but there were none"
        assert data_from_database == [(row['name'], row['id']) for row in ROWS_IN_DATABASE]

    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(pipe_name=pipe_name, stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
@sdc_enterprise_lib_min_version({'snowflake': '1.9.0'})
@pytest.mark.parametrize('number_of_threads', [3, 5])
@pytest.mark.parametrize('pipe_auto_create', [True, False])
def test_basic_snowpipe_multithread(sdc_builder, sdc_executor, snowflake, number_of_threads, pipe_auto_create):
    """Test for Snowflake destination target stage using multithreaded origin, and testing pipe auto create.
    Data is inserted into Snowflake using the pipeline.
    After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    We assert the data from the client to what has been ingested by the Snowflake pipeline.
    Here we set the Snowpipe to ingest data. Note that when Data Drift is enabled, Snowpipe is not used.

    NOTE: When using GCS staging, snowpipe tests are most likely going to fail, due to limitations in Snowflake
    capabilities: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-ts.html#loads-from-google-cloud-storage-delayed-or-files-missed
    So we just skip them. But if there is the need of trying Snowpipe+gcs, they can eventually pass.

    The pipeline looks like:
    Snowflake pipeline:
        dev_data_generator  >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'
    pipe_name = f'STF_PIPE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Create a table, stage and pipe in Snowflake.
    table = snowflake.create_table(table_name.lower())
    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)
    snowflake.create_pipe(pipe_name, stage_name, table_name)

    # Build the pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(delay_between_batches=100,
                                      batch_size=1,
                                      number_of_threads=number_of_threads,
                                      fields_to_generate=[{"type": "INTEGER", "field": "ID"},
                                                          {"type": "POKEMON", "field": "NAME"}])

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(pipe=pipe_name,
                                         purge_stage_file_after_ingesting=False,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         use_snowpipe=True,
                                         data_drift_enabled=False,
                                         snowpipe_auto_create=pipe_auto_create)

    dev_data_generator >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline=pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 5)
        sdc_executor.stop_pipeline(pipeline)
        pipeline_history = sdc_executor.get_pipeline_history(pipeline)

        def query_function():
            return engine.execute(table.select())

        data_from_database = wait_for_snowpipe_data_ingestion(query_function)

        assert len(data_from_database) > 0, "There should be some data in the database, but there were none"
        records_output_count = pipeline_history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        assert records_output_count <= len(data_from_database), "Output records count should be lower than data from" \
                                                                " snowflake but it wasn't"
        records_input_count = pipeline_history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        assert records_input_count <= len(data_from_database), "Input records count should be lower than data from " \
                                                               "snowflake but it wasn't"

    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(pipe_name=pipe_name, stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_enterprise_lib_min_version({'snowflake': '1.2.0'})
@pytest.mark.parametrize('pk', [True, False])
def test_CDC_Snowflake(sdc_builder, sdc_executor, snowflake, pk):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    We assert the data from the client to what has been ingested by the Snowflake pipeline.
    The data is in CDC format. To do that:
    - A dev raw data source stage generates the data adding an 'op' field
    - An expression evaluator adds the op field in the header
    - A field remover removes the op field
    - A Snowflake stage adds the data

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Create a table and stage in Snowflake.
    table = snowflake.create_table(table_name.lower())
    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw Data Source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = ('\n').join((json.dumps(row) for row in CDC_ROWS_IN_DATABASE))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "${record:value('/OP')}"}])

    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/OP']

    # Build Snowflake
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')

    table_key_columns=[{
        "keyColumns": [
            "ID"
        ],
        "table": table_name
    }]
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         processing_cdc_data=True)

    # Primary key properties were modified in 1.12
    if Version(snowflake_destination.stage_version) < Version('14'):
        snowflake_destination.set_attributes(get_primary_key_information_from_snowflake=pk,
                                             tables_key_columns=table_key_columns)
    else:
        snowflake_destination.set_attributes(primary_key_location="SNOWFLAKE" if pk else "TABLE",
                                             table_key_columns=table_key_columns)

    dev_raw_data_source >> expression_evaluator >> field_remover >> snowflake_destination
    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=300)
        result = engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()
        assert data_from_database == [(row['NAME'], row['ID']) for row in CDC_ROWS_IN_DATABASE]
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
@sdc_enterprise_lib_min_version({'snowflake': '1.2.0'})
@pytest.mark.parametrize('stage_location', ["INTERNAL", "AWS_S3", "AZURE", "GCS"])
def test_cdc_snowflake_multiple_ops(sdc_builder, sdc_executor, snowflake, stage_location):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    The first table has the rows id=1 and id=3. The second table just has row id=2.
    We assert the data from the client to what has been ingested by the Snowflake pipeline.
    The data is in CDC format. To do that:
    - A dev raw data source stage generates the data adding an 'op' field and a table name
    - An expression evaluator adds the op field in the header
    - A field remover removes the op field
    - A Snowflake stage adds the data

    Two tables are created by Pipeline
    The table names are included in each row

    We test for the different staging areas available.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> snowflake_destination
    """

    table_name_1 = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    table_name_2 = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'

    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    CDC_ROWS_MULT_OPS = [
        {'TABLE': table_name_1, 'OP': 1, 'ID': 1, 'NAME': 'Rogelio Federer'},
        {'TABLE': table_name_2, 'OP': 1, 'ID': 2, 'NAME': 'Rafa Nadal'},
        {'TABLE': table_name_1, 'OP': 1, 'ID': 3, 'NAME': 'Domi Thiem'},
        {'TABLE': table_name_2, 'OP': 1, 'ID': 4, 'NAME': 'Juan Del Potro'},
        {'TABLE': table_name_1, 'OP': 3, 'ID': 1, 'NAME': 'Roger Federer'},
        {'TABLE': table_name_2, 'OP': 3, 'ID': 2, 'NAME': 'Rafael Nadal'},
        {'TABLE': table_name_1, 'OP': 3, 'ID': 3, 'NAME': 'Dominic Thiem'},
        {'TABLE': table_name_2, 'OP': 2, 'ID': 4, 'NAME': 'Juan Del Potro'}
    ]

    table_key_columns = [
        {"keyColumns": ["ID"], "table": table_name_1},
        {"keyColumns": ["ID"], "table": table_name_2}
    ]

    engine = snowflake.engine

    # Connect to two tables (tables are created by the pipeline)
    table_1 = snowflake.describe_table(table_name_1.lower())
    table_2 = snowflake.describe_table(table_name_2.lower())

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location=stage_location)

    # Build the pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = ('\n').join((json.dumps(row) for row in CDC_ROWS_MULT_OPS))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(
        header_attribute_expressions=[{'attributeToSet': 'sdc.operation.type',
                                       'headerAttributeExpression': "${record:value('/OP')}"}])
    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/OP']

    # Build Snowflake
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(stage_location=stage_location,
                                         purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table="${record:value('/TABLE')}",
                                         row_field='/', column_fields_to_ignore='TABLE',
                                         s3_encryption="S3",
                                         table_auto_create=True,
                                         processing_cdc_data=True)

    # Primary key properties were modified in 1.12
    if Version(snowflake_destination.stage_version) < Version('14'):
        snowflake_destination.set_attributes(tables_key_columns=table_key_columns)
    else:
        snowflake_destination.set_attributes(primary_key_location="TABLE",
                                             table_key_columns=table_key_columns)

    dev_raw_data_source >> expression_evaluator >> field_remover >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=600)

        # ID 1 Row and ID 3 Row are in Table 1.
        # It is done configuring field TABLE to Table 1 in input data.
        result = engine.execute(table_1.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()

        # Data from Snowflake Database is compared with expected Data.
        # Expected Data is ROWS_IN_DATABASE with ID=1 and ID=3
        assert data_from_database[0] == [(row['name'], row['id']) for row in ROWS_IN_DATABASE][0]
        assert data_from_database[1] == [(row['name'], row['id']) for row in ROWS_IN_DATABASE][2]

        # ID 2 Row is in Table 2. It is done configuring field TABLE to Table 1 in input data.
        # ID 4 Row is added and deleted id CDC Input. ID 4 should not be added.

        result = engine.execute(table_2.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()

        # Data from Snowflake Database is compared with expected Data.
        # Expected Data is ROWS_IN_DATABASE with ID=2. Just one row.
        assert data_from_database[0] == [(row['name'], row['id']) for row in ROWS_IN_DATABASE][1]
        assert len(data_from_database) == 1

    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)
        table_1.drop(engine)
        table_2.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
@sdc_enterprise_lib_min_version({'snowflake': '1.7.0'})
def test_cdc_snowflake_multiple_ops_two_batches(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    We insert data in two runs.
    We assert the data from the client to what has been ingested by the Snowflake pipeline.
    The data is in CDC format. To do that:
    - A dev raw data source stage generates the data adding an 'op' field and a table name
    - An expression evaluator adds the op field in the header
    - A field remover removes the op field
    - A Snowflake stage adds the data

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> snowflake_destination

    This test is for SNOWFLAKE-241, it set up two batches
    -- Bacth 0
        UPINSERT A
        UPINSERT A

    -- Batch 1
        UPINSERT A
        UPINSERT A
        DELETE A

    """

    table_name_1 = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    CDC_ROWS_MULT_OPS_1 = [
        {'TABLE': table_name_1, 'OP': 4, 'ID': 1, 'NAME': 'Rogelio Federer'},
        {'TABLE': table_name_1, 'OP': 4, 'ID': 1, 'NAME': 'Rafa Nadal'}]
    CDC_ROWS_MULT_OPS_2 = [
        {'TABLE': table_name_1, 'OP': 4, 'ID': 1, 'NAME': 'Domi Thiem'},
        {'TABLE': table_name_1, 'OP': 4, 'ID': 1, 'NAME': 'Juan Del Potro'},
        {'TABLE': table_name_1, 'OP': 2, 'ID': 1, 'NAME': 'Juan Del Potro'}]

    table_key_columns = [
        {"keyColumns": ["ID"], "table": table_name_1}
    ]

    engine = snowflake.engine

    # Connect table (table is created by the pipeline)
    table_1 = snowflake.describe_table(table_name_1.lower())

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = ('\n').join((json.dumps(row) for row in CDC_ROWS_MULT_OPS_1))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(
        header_attribute_expressions=[{'attributeToSet': 'sdc.operation.type',
                                       'headerAttributeExpression': "${record:value('/OP')}"}])
    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/OP']

    # Build Snowflake
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table="${record:value('/TABLE')}",
                                         row_field='/', column_fields_to_ignore='TABLE',
                                         s3_encryption="S3",
                                         table_auto_create=True,
                                         processing_cdc_data=True)

    # Primary key properties were modified in 1.12
    if Version(snowflake_destination.stage_version) < Version('14'):
        snowflake_destination.set_attributes(tables_key_columns=table_key_columns)
    else:
        snowflake_destination.set_attributes(primary_key_location="TABLE",
                                             table_key_columns=table_key_columns)

    dev_raw_data_source >> expression_evaluator >> field_remover >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=600)

        # ID 1 Row is in Table 1.
        result = engine.execute(table_1.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()

        # Data from Snowflake Database is compared with expected Data.
        # Expected Data is 1 Row with name Rafa Nadal
        assert data_from_database == [(row['NAME'], row['ID']) for row in CDC_ROWS_MULT_OPS_1[1:2]]

        raw_data = ('\n').join((json.dumps(row) for row in CDC_ROWS_MULT_OPS_2))
        pipeline[0].configuration.update({'rawData': raw_data})
        sdc_executor.update_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=600)

        # Data from database is empty because of the Delete (Op 2)
        result = engine.execute(table_1.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()
        assert data_from_database == []

    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)
        table_1.drop(engine)
        engine.dispose()


@snowflake
@pytest.mark.parametrize('primary_key_columns', [
    ['ID'],
    ['NAME'],
    ['ID', 'NAME']
])
@sdc_enterprise_lib_min_version({'snowflake': '1.12.0'})
def test_cdc_snowflake_create_primary_key(sdc_builder, sdc_executor, snowflake, primary_key_columns):
    """Very similar to test_CDC_Snowflake. Table is auto created, and we will also assert
    that the key columns are created as primary keys.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Table is created by the pipeline
    table = snowflake.describe_table(table_name)

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw Data Source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = ('\n').join((json.dumps(row) for row in CDC_ROWS_IN_DATABASE))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "${record:value('/OP')}"}])

    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/OP']

    # Build Snowflake
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')

    snowflake_destination.set_attributes(primary_key_location="TABLE",
                                         table_key_columns=[{
                                             "keyColumns": primary_key_columns,
                                             "table": table_name
                                         }],
                                         purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         processing_cdc_data=True,
                                         table_auto_create=True)

    dev_raw_data_source >> expression_evaluator >> field_remover >> snowflake_destination
    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()
        assert data_from_database == [(row['NAME'], row['ID']) for row in CDC_ROWS_IN_DATABASE]

        # and we also assert that the pk columns created are PK and NOT NULL
        result = engine.execute(f'DESC TABLE {table_name};')
        metadata_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        assert len(metadata_from_database) == 2
        for column in metadata_from_database:
            name = column[0]
            nullable = column[3]
            primary_key = column[5]
            if name in primary_key_columns:
                assert nullable == 'N'
                assert primary_key == 'Y'
            else:
                assert nullable == 'Y'
                assert primary_key == 'N'
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


@snowflake
@pytest.mark.parametrize('primary_key_columns', [
    ['ID'],
    ['NAME'],
    ['ID', 'NAME']
])
@sdc_enterprise_lib_min_version({'snowflake': '1.12.0'})
def test_cdc_snowflake_update_primary_key(sdc_builder, sdc_executor, snowflake, primary_key_columns):
    """Very similar to test_cdc_snowflake_create_primary_key, but we will create the
    table beforehand with ID as the primary key column, and then use NAME as the key column,
    so the primary key of the table gets updated. Note that columns added to the pk cannot be
    NOT NULL, and that columns that where NOT NULL getting dropped from the pk also drop that constraint.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Create a table and stage in Snowflake.
    table = snowflake.create_table(table_name.lower())
    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw Data Source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = ('\n').join((json.dumps(row) for row in CDC_ROWS_IN_DATABASE))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "${record:value('/OP')}"}])

    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/OP']

    # Build Snowflake
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')

    snowflake_destination.set_attributes(primary_key_location="TABLE",
                                         table_key_columns=[{
                                             "keyColumns": primary_key_columns,
                                             "table": table_name
                                         }],
                                         purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         processing_cdc_data=True,
                                         table_auto_create=True)

    dev_raw_data_source >> expression_evaluator >> field_remover >> snowflake_destination
    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()
        assert data_from_database == [(row['NAME'], row['ID']) for row in CDC_ROWS_IN_DATABASE]

        # and we also assert that the pk columns created are PK and NOT NULL
        result = engine.execute(f'DESC TABLE {table_name};')
        metadata_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        assert len(metadata_from_database) == 2
        for column in metadata_from_database:
            name = column[0]
            nullable = column[3]
            primary_key = column[5]
            if name in primary_key_columns:
                # ID is the default pk column created by STF as NOT NULL
                if name == 'ID':
                    assert nullable == 'N'
                else:
                    # if we update the pk, it has to remain nullable to maintain consistency
                    assert nullable == 'Y'
                assert primary_key == 'Y'
            else:
                assert nullable == 'Y'
                assert primary_key == 'N'
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_enterprise_lib_min_version({'snowflake': '1.12.0'})
def test_cdc_snowflake_jdbc_header(sdc_builder, sdc_executor, snowflake):
    """We will set up the headers the same way JDBC origins do. We will have the primary keys and
    pk values updates as well.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> snowflake_destination
    """
    primary_key_columns = ['TYPE', 'ID']

    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Table is created by the pipeline
    table = snowflake.describe_table(table_name.lower())

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw Data Source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    rows = CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY
    for row, header in zip(rows, CDC_PK_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY_HEADER):
        row['HEADER'] = header
    raw_data = ('\n').join((json.dumps(row) for row in rows))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "${record:value('/HEADER/sdc.operation.type')}"},
        {'attributeToSet': f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID',
         'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_OLD_VALUE + ".ID')}"},
        {'attributeToSet': f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID',
         'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_NEW_VALUE + ".ID')}"},
        {'attributeToSet': f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE',
         'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_OLD_VALUE + ".TYPE')}"},
        {'attributeToSet': f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE',
         'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_NEW_VALUE + ".TYPE')}"},
        {'attributeToSet': f'{PRIMARY_KEY_SPECIFICATION}', 'headerAttributeExpression': '{\"ID\":{}, \"TYPE\":{}}'}
    ])

    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/HEADER']

    # Build Snowflake
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')

    snowflake_destination.set_attributes(primary_key_location="HEADER",
                                         purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         processing_cdc_data=True,
                                         table_auto_create=True)

    dev_raw_data_source >> expression_evaluator >> field_remover >> snowflake_destination
    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(f'select * from {table_name}')
        data_from_database = result.fetchall()
        result.close()
        # we are chain updating, it has to equal last record sent
        expected_data = rows[6]
        assert data_from_database == [(expected_data['TYPE'], expected_data['ID'],
                                       expected_data['NAME'], expected_data['SURNAME'], expected_data['ADDRESS'])]

        # and we also assert that the pk columns created are PK and NOT NULL
        result = engine.execute(f'desc table {table_name}')
        metadata_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        assert len(metadata_from_database) == 5
        for column in metadata_from_database:
            name = column[0]
            nullable = column[3]
            primary_key = column[5]
            if name in primary_key_columns:
                assert nullable == 'N'
                assert primary_key == 'Y'
            else:
                assert nullable == 'Y'
                assert primary_key == 'N'
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
def test_basic_datadrift(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    Two pipelines are run, the second one with a newer column.
    The expected behavior is to have six rows.

    The pipelines look like:
    Snowflake pipelines:
        dev_raw_data_source  >> snowflake_destination
        dev_raw_data_source2  >> snowflake_destination2
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Create a table and stage in Snowflake.
    table = snowflake.create_table(table_name.lower())
    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the first pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = ('\n').join((json.dumps(row) for row in ROWS_IN_DATABASE))
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data,
                                       stop_after_first_batch=True)

    # Build Snow Flake
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name)
    dev_raw_data_source >> snowflake_destination

    # Build first pipeline
    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

    # Build the second pipeline with created entities with new columns in Snowflake stage configurations.
    pipeline_builder2 = sdc_builder.get_pipeline_builder()

    # Build Dev Raw with different columns
    dev_raw_data_source2 = pipeline_builder2.add_stage('Dev Raw Data Source')
    raw_data = ('\n').join((json.dumps(row) for row in DRIFT_ROWS_IN_DATABASE))
    dev_raw_data_source2.set_attributes(data_format='JSON', raw_data=raw_data,
                                        stop_after_first_batch=True)

    # Build Second Snow Flake
    snowflake_destination2 = pipeline_builder2.add_stage('Snowflake', type='destination')
    snowflake_destination2.set_attributes(snowflake_stage_name=stage_name, table=table_name)

    dev_raw_data_source2 >> snowflake_destination2

    # Build second pipeline
    pipeline2 = (pipeline_builder2.build()).configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline2)
    sdc_executor.start_pipeline(pipeline=pipeline2).wait_for_finished()
    try:
        s = text('Select name, id from ' + table_name + ' where id <= 3 order by id')
        result = engine.execute(s)
        data_from_database = result.fetchall()
        result.close()
        assert data_from_database == [(row['name'], row['id']) for row in ROWS_IN_DATABASE]

        s = text('Select name, id, ranking from ' + table_name + ' where id > 3 order by id')
        result = engine.execute(s)
        data_from_database = result.fetchall()
        result.close()
        assert data_from_database == [(row['name'], row['id'], row['ranking']) for row in
                                      DRIFT_ROWS_IN_DATABASE]

    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
@sdc_enterprise_lib_min_version({'snowflake': '1.7.0'})
def test_datadrift_add_column_error(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    Two pipelines are run, the second one with a newer column which has an invalid identifier that makes snowflake
    throw an exception.

    The pipelines look like:
    Snowflake pipelines:
        dev_raw_data_source  >> [snowflake_destination, wiretap.destination]
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Create a table and stage in Snowflake.
    table = snowflake.create_table(table_name.lower())
    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the first pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = ('\n').join((json.dumps(row) for row in ROWS_IN_DATABASE))
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data,
                                       stop_after_first_batch=True)

    # Build Snow Flake
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name)

    # Build wiretap
    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> [snowflake_destination, wiretap.destination]

    # Build pipeline
    pipeline = pipeline_builder.build().configure_for_environment(snowflake)

    # Run pipeline first time
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

    # Modify raw data source
    dev_raw_data_source = pipeline.stages.get(label=dev_raw_data_source.label)
    raw_data = ('\n').join((json.dumps(row) for row in DRIFT_ROWS_IN_DATABASE_WRONG_COLUMN))
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data,
                                       stop_after_first_batch=True)

    # Update pipeline and run second time
    sdc_executor.update_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
    try:
        select_query_inserted_items = text('Select name, id from ' + table_name + ' where id <= 3 order by id')
        result = engine.execute(select_query_inserted_items)
        data_from_database = result.fetchall()
        result.close()
        assert data_from_database == [(row['name'], row['id']) for row in ROWS_IN_DATABASE]

        select_query_no_items = text('Select name, id from ' + table_name + ' where id > 3 order by id')
        result = engine.execute(select_query_no_items)
        data_from_database = result.fetchall()
        result.close()
        assert len(data_from_database) == 0
        assert len(wiretap.error_records) == 1
        assert 'SNOWFLAKE_62' in wiretap.error_records[0].header['errorCode']
        assert 'Error when adding column' in wiretap.error_records[0].header['errorMessage']

    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
def test_Snowflake_Multitable(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    We assert the data from the client to what has been ingested by the Snowflake pipeline.
    The first column of the Dev Raw Data Source includes de Table.
    The column with the Table Name is excluded in the Snowflake target.

    Table is created by Pipeline

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >>   snowflake_destination
    """

    table_1_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 10)}'
    table_2_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 10)}'
    table_3_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 10)}'

    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 10)}'

    MULTITABLE_ROWS_IN_DATABASE = [
        {'table': table_1_name, 'id': 1, 'name': 'Roger Federer'},
        {'table': table_2_name, 'id': 2, 'name': 'Rafael Nadal'},
        {'table': table_3_name, 'id': 3, 'name': 'Dominic Thiem'}
    ]

    engine = snowflake.engine

    # Create a table and stage in Snowflake.
    table_1 = snowflake.create_table(table_1_name.lower())
    table_2 = snowflake.create_table(table_2_name.lower())
    table_3 = snowflake.create_table(table_3_name.lower())

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = ('\n').join((json.dumps(row) for row in MULTITABLE_ROWS_IN_DATABASE))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    # Build Snowflake
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(snowflake_stage_name=stage_name,
                                         table="${record:value('/table')}",
                                         row_field='/', column_fields_to_ignore='table'
                                         )

    dev_raw_data_source >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        result = engine.execute(table_1.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()
        assert data_from_database == [(MULTITABLE_ROWS_IN_DATABASE[0]['name'], MULTITABLE_ROWS_IN_DATABASE[0]['id'])]

        result = engine.execute(table_2.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()
        assert data_from_database == [(MULTITABLE_ROWS_IN_DATABASE[1]['name'], MULTITABLE_ROWS_IN_DATABASE[1]['id'])]

        result = engine.execute(table_3.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()
        assert data_from_database == [(MULTITABLE_ROWS_IN_DATABASE[2]['name'], MULTITABLE_ROWS_IN_DATABASE[2]['id'])]

    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)
        table_1.drop(engine)
        table_2.drop(engine)
        table_3.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
def test_snowflake_multitable_volume_multithread(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    We assert there are no records discarded.
    The table is created by the pipeline.
    Five threads are used.
    Five connection pool are used."""

    table_name_1 = f'C_STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    table_name_2 = f'B_STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'

    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Connect to two tables (tables are created by the pipeline).
    table_1 = snowflake.describe_table(table_name_1.lower())
    table_2 = snowflake.describe_table(table_name_2.lower())

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.fields_to_generate = [
        {'field': 'REGION', 'type': 'STRING'},
        {'field': 'COUNTRY', 'type': 'ADDRESS_COUNTRY'},
        {'field': 'SALES_CHANNEL', 'type': 'STRING'},
        {'field': 'ORDER_PRIORITY', 'type': 'STRING'},
        {'field': 'ORDER_DATE', 'type': 'DATE'},
        {'field': 'ORDER_ID', 'type': 'INTEGER'},
        {'field': 'SHIP_DATE', 'type': 'DATE'},
        {'field': 'UNITS_SOLD', 'type': 'INTEGER'},
        {'field': 'UNIT_PRICE', 'type': 'FLOAT'},
        {'field': 'UNIT_COST', 'type': 'FLOAT'},
        {'field': 'TOTAL_REVENUE', 'type': 'DOUBLE'},
        {'field': 'TOTAL_COST', 'type': 'DOUBLE'},
        {'field': 'TOTAL_PROFIT', 'type': 'DOUBLE'},
        {'field': 'TABLE_TYPE', 'type': 'BOOLEAN'}]

    dev_data_generator.set_attributes(delay_between_batches=10,
                                      batch_size=1000,
                                      number_of_threads=5)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.field_expressions = [
        dict(fieldToSet='/ITEM_TYPE',
             expression=f'${{record:value("/TABLE_TYPE")?"{table_name_1}":"{table_name_2}"}}')]

    # Build Field Remover.
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/TABLE_TYPE']

    # Build Snowflake.
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(snowflake_stage_name=stage_name,
                                         table="${record:value('/ITEM_TYPE')}",
                                         row_field='/', column_fields_to_ignore='ITEM_TYPE',
                                         table_auto_create=True,
                                         connection_pool_size=5)

    dev_data_generator >> expression_evaluator >> field_remover >> snowflake_destination
    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(pipeline=pipeline, force=True)

        result = engine.execute(f'select * from {table_name_1}')
        data_from_database_1 = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()
        assert len(data_from_database_1) > 0

        result = engine.execute(f'select * from {table_name_2}')
        data_from_database_2 = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()
        assert len(data_from_database_2) > 0

        pipeline_history = sdc_executor.get_pipeline_history(pipeline)
        records_output_count = pipeline_history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        assert records_output_count <= len(data_from_database_1) + len(data_from_database_2)
        records_input_count = pipeline_history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        assert records_input_count <= len(data_from_database_1) + len(data_from_database_2)

    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)
        table_1.drop(engine)
        table_2.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
@sdc_enterprise_lib_min_version({'snowflake': '1.10.0'})
def test_snowflake_multitable_auto_create_pipe(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    We assert there are no records discarded.
    The table is created by the pipeline.
    Five threads are used.
    Five connection pool are used.

    NOTE: When using GCS staging, snowpipe tests are most likely going to fail, due to limitations in Snowflake
    capabilities: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-ts.html#loads-from-google-cloud-storage-delayed-or-files-missed
    So we just skip them. But if there is the need of trying Snowpipe+gcs, they can eventually pass.
    """
    table_name_1 = f'C_STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    table_name_2 = f'B_STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'

    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Connect to two tables (tables are created by the pipeline).
    table_1 = snowflake.describe_table(table_name_1.lower())
    table_2 = snowflake.describe_table(table_name_2.lower())

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.fields_to_generate = [
        {'field': 'REGION', 'type': 'STRING'},
        {'field': 'COUNTRY', 'type': 'ADDRESS_COUNTRY'},
        {'field': 'SALES_CHANNEL', 'type': 'STRING'},
        {'field': 'ORDER_PRIORITY', 'type': 'STRING'},
        {'field': 'ORDER_DATE', 'type': 'DATE'},
        {'field': 'ORDER_ID', 'type': 'INTEGER'},
        {'field': 'SHIP_DATE', 'type': 'DATE'},
        {'field': 'UNITS_SOLD', 'type': 'INTEGER'},
        {'field': 'UNIT_PRICE', 'type': 'FLOAT'},
        {'field': 'UNIT_COST', 'type': 'FLOAT'},
        {'field': 'TOTAL_REVENUE', 'type': 'DOUBLE'},
        {'field': 'TOTAL_COST', 'type': 'DOUBLE'},
        {'field': 'TOTAL_PROFIT', 'type': 'DOUBLE'},
        {'field': 'TABLE_TYPE', 'type': 'BOOLEAN'}]

    dev_data_generator.set_attributes(delay_between_batches=10,
                                      batch_size=1000,
                                      number_of_threads=5)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.field_expressions = [
        dict(fieldToSet='/ITEM_TYPE',
             expression=f'${{record:value("/TABLE_TYPE")?"{table_name_1}":"{table_name_2}"}}')]

    # Build Field Remover.
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/TABLE_TYPE']

    # Build Snowflake.
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(snowflake_stage_name=stage_name,
                                         table="${record:value('/ITEM_TYPE')}",
                                         row_field='/', column_fields_to_ignore='ITEM_TYPE',
                                         data_drift_enabled=False,
                                         table_auto_create=True,
                                         use_snowpipe=True,
                                         snowpipe_auto_create=True,
                                         pipe_prefix='stfpipe')

    dev_data_generator >> expression_evaluator >> field_remover >> snowflake_destination
    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(pipeline=pipeline, force=True)

        def query_function():
            return engine.execute(f'select * from {table_name_1}')

        data_from_database_1 = wait_for_snowpipe_data_ingestion(query_function)
        assert len(data_from_database_1) > 0, "There should be some data in the database, but there were none"

        def query_function():
            return engine.execute(f'select * from {table_name_2}')

        data_from_database_2 = wait_for_snowpipe_data_ingestion(query_function)
        assert len(data_from_database_2) > 0, "There should be some data in the database, but there were none"

        pipeline_history = sdc_executor.get_pipeline_history(pipeline)
        records_output_count = pipeline_history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        assert records_output_count <= len(data_from_database_1) + len(data_from_database_2)
        records_input_count = pipeline_history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        assert records_input_count <= len(data_from_database_1) + len(data_from_database_2)

    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)
        table_1.drop(engine)
        table_2.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
@sdc_enterprise_lib_min_version({'snowflake': '1.10.0'})
def test_snowpipe_invalid_user_stage(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage. We should get a validation error when using snowpipe and ~ as stage.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    raw_data = '\n'.join(json.dumps(row) for row in ROWS_IN_DATABASE)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(snowflake_stage_name="~",
                                         table=table_name,
                                         data_drift_enabled=False,
                                         table_auto_create=True,
                                         use_snowpipe=True,
                                         snowpipe_auto_create=True,
                                         pipe_prefix='stfpipe')

    dev_raw_data_source >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)

    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        pytest.fail('Test should have raised an Exception with SNOWFLAKE_76 error, but did not')
    except Exception as e:
        assert 'SNOWFLAKE_76' \
               in e.message, f'Test should have raised an Exception with SNOWFLAKE_76 error, but raised {e.message}'


@snowflake
@sdc_min_version('3.7.0')
def test_failing_table_el_eval_snowflake(sdc_builder, sdc_executor, snowflake):
    """Test Snowflake destination sends records to error when table EL evaluation fails
    - A dev raw data source stage generates the data
    - A Snowflake stage tries to send the data but records go to error

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >>  >> snowflake_destination
    """
    table_name_1 = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    table_name_2 = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'

    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    data = [
        {'TABLE': table_name_1, 'ID': 1, 'NAME': 'Rogelio Federer'},
        {'TABLE': table_name_2, 'ID': 2, 'NAME': 'Rafa Nadal'},
        {'TABLE': table_name_1, 'ID': 3, 'NAME': 'Domi Thiem'},
        {'TABLE': table_name_2, 'ID': 4, 'NAME': 'Juan Del Potro'},
        {'TABLE': table_name_1, 'ID': 1, 'NAME': 'Roger Federer'},
        {'TABLE': table_name_2, 'ID': 2, 'NAME': 'Rafael Nadal'},
        {'TABLE': table_name_1, 'ID': 3, 'NAME': 'Dominic Thiem'},
        {'TABLE': table_name_2, 'ID': 4, 'NAME': 'Juan Del Potro'}
    ]

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw stage.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = '\n'.join((json.dumps(row) for row in data))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    # Build Snowflake stage.
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table="${record:fail()}",
                                         row_field='/', column_fields_to_ignore='TABLE',
                                         table_auto_create=True)

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> [snowflake_destination, wiretap.destination]

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(data) == len(wiretap.error_records)
        for error_record in wiretap.error_records:
            assert 'SNOWFLAKE_60' == error_record.header['errorCode']
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)


@snowflake
@sdc_enterprise_lib_min_version({'snowflake': '1.5.0'})
def test_table_el_eval_snowflake(sdc_builder, sdc_executor, snowflake):
    """Test Snowflake destination is able to split into different tables based on EL using different threads
    - A directory origin stage generates the data
    - A Snowflake stage tries to send the data but records go to error

    The pipeline looks like:
    Snowflake pipeline:
        directory >> snowflake_destination
        directory >= file_finished_finisher
    """
    table_name = f'stf_table_{get_random_string(string.ascii_lowercase, 5)}'
    table_name_1 = f'{table_name}_1'
    table_name_2 = f'{table_name}_2'
    table_name_3 = f'{table_name}_3'
    table_name_4 = f'{table_name}_4'
    table_name_5 = f'{table_name}_5'

    table_1 = sqlalchemy.Table(table_name_1,
                               sqlalchemy.MetaData(),
                               sqlalchemy.Column('ID', sqlalchemy.Integer),
                               sqlalchemy.Column('NAME', sqlalchemy.String))
    table_2 = sqlalchemy.Table(table_name_2,
                               sqlalchemy.MetaData(),
                               sqlalchemy.Column('ID', sqlalchemy.Integer),
                               sqlalchemy.Column('NAME', sqlalchemy.String))
    table_3 = sqlalchemy.Table(table_name_3,
                               sqlalchemy.MetaData(),
                               sqlalchemy.Column('ID', sqlalchemy.Integer),
                               sqlalchemy.Column('NAME', sqlalchemy.String))
    table_4 = sqlalchemy.Table(table_name_4,
                               sqlalchemy.MetaData(),
                               sqlalchemy.Column('ID', sqlalchemy.Integer),
                               sqlalchemy.Column('NAME', sqlalchemy.String))
    table_5 = sqlalchemy.Table(table_name_5,
                               sqlalchemy.MetaData(),
                               sqlalchemy.Column('ID', sqlalchemy.Integer),
                               sqlalchemy.Column('NAME', sqlalchemy.String))

    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    data = []

    engine = snowflake.engine

    for i in range(0, 1000):
        data.append({'TABLE': f'{table_name}_{random.randint(1, 5)}', 'ID': i, 'NAME': get_random_string()})

    raw_data = '\n'.join((json.dumps(row) for row in data))

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string())

    sdc_executor.execute_shell(f'mkdir -p {tmp_directory}')
    sdc_executor.write_file(os.path.join(tmp_directory, 'sdc1.txt'), raw_data)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='JSON', file_name_pattern='sdc*.txt', file_name_pattern_mode='GLOB',
                             files_directory=tmp_directory, process_subdirectories=True, read_order='TIMESTAMP',
                             batch_size_in_recs=50, number_of_threads=5)

    file_finished_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    file_finished_finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'finished-file'}"])

    # Build Snowflake stage.
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         connection_pool_size=5,
                                         snowflake_stage_name=stage_name,
                                         table="${record:value('/TABLE')}",
                                         row_field='/')

    directory >> snowflake_destination
    directory >= file_finished_finisher

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        table_1.create(engine)
        table_2.create(engine)
        table_3.create(engine)
        table_4.create(engine)
        table_5.create(engine)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=600)

        result = engine.execute(table_1.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()
        inserted_data = [(row['ID'], row['NAME']) for row in list(filter(lambda x: x['TABLE'] == table_name_1, data))]
        assert len(inserted_data) == len(data_from_database)
        assert inserted_data == data_from_database

        result = engine.execute(table_2.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()
        inserted_data = [(row['ID'], row['NAME']) for row in list(filter(lambda x: x['TABLE'] == table_name_2, data))]
        assert len(inserted_data) == len(data_from_database)
        assert inserted_data == data_from_database

        result = engine.execute(table_3.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        inserted_data = [(row['ID'], row['NAME']) for row in list(filter(lambda x: x['TABLE'] == table_name_3, data))]
        assert len(inserted_data) == len(data_from_database)
        assert inserted_data == data_from_database

        result = engine.execute(table_4.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()
        inserted_data = [(row['ID'], row['NAME']) for row in list(filter(lambda x: x['TABLE'] == table_name_4, data))]
        assert len(inserted_data) == len(data_from_database)
        assert inserted_data == data_from_database

        result = engine.execute(table_5.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()
        inserted_data = [(row['ID'], row['NAME']) for row in list(filter(lambda x: x['TABLE'] == table_name_5, data))]
        assert len(inserted_data) == len(data_from_database)
        assert inserted_data == data_from_database

    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)
        table_1.drop(engine)
        table_2.drop(engine)
        table_3.drop(engine)
        table_4.drop(engine)
        table_5.drop(engine)


@snowflake
@sdc_min_version('3.7.0')
@sdc_enterprise_lib_min_version({'snowflake': '1.5.0'})
def test_instance_profile_credentials(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    We assert the data from the client to what has been ingested by the Snowflake pipeline.
    Instead of credentials we use IAMRoles to access to S3 from SDC. Snowflake is still using the credentials.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # Create a table and stage in Snowflake.
    table = snowflake.create_table(table_name.lower())
    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    raw_data = '\n'.join(json.dumps(row) for row in ROWS_IN_DATABASE)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         use_instance_profile=True)

    dev_raw_data_source >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)

    snowflake_destination.set_attributes(aws_access_key_id="", aws_secret_key_id="")
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == [(row['name'], row['id']) for row in ROWS_IN_DATABASE]
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
def test_stage_and_table_different_schema(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    We create the stage and the table with different schema.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_schema = f'STF_SCHEMA_{get_random_string(string.ascii_uppercase, 5)}'

    # Create a table and stage in Snowflake.
    table = snowflake.create_table(table_name.lower())
    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_schema)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    raw_data = '\n'.join(json.dumps(row) for row in ROWS_IN_DATABASE)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         stage_schema=stage_schema)

    dev_raw_data_source >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == [(row['name'], row['id']) for row in ROWS_IN_DATABASE]
    finally:
        snowflake.drop_entities(stage_name=stage_name, schema_name=stage_schema)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
@sdc_enterprise_lib_min_version({'snowflake': '1.8.0'})
@pytest.mark.parametrize('upper_case_schema_and_field_names', [True, False])
def test_stage_table_schema_mixed_case(sdc_builder, sdc_executor, snowflake, upper_case_schema_and_field_names):
    """Test for Snowflake destination target stage when names have mixed case letters. Data is inserted into Snowflake.
    Similar to test above, but including mixed case letters. If it gets forced to upper case, the test fails saying
    there is no such table; else it reads from snowflake as expected.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >> snowflake_destination
    """
    table_name = f'STF_table_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_stage_{get_random_string(string.ascii_uppercase, 5)}'
    stage_schema = f'STF_schema_{get_random_string(string.ascii_uppercase, 5)}'

    # Raw data needs to be upper cased so we can compare it (else the fields in Snowflake are upper case but the
    # test values are not).
    UPPER_CASED_ROWS_IN_DATABASE = [
        {'ID': 1, 'NAME': 'Roger Federer'},
        {'ID': 2, 'NAME': 'Rafael Nadal'},
        {'ID': 3, 'NAME': 'Dominic Thiem'}
    ]

    # Create a table and stage in Snowflake.
    table = snowflake.create_table(table_name)
    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_schema)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    raw_data = '\n'.join(json.dumps(row) for row in UPPER_CASED_ROWS_IN_DATABASE)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         stage_schema=stage_schema,
                                         upper_case_schema_and_field_names=upper_case_schema_and_field_names)

    dev_raw_data_source >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        if upper_case_schema_and_field_names:
            pytest.fail('Should not reach this point')
        else:
            result = engine.execute(table.select())
            data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
            result.close()
            assert data_from_database == [(row['NAME'], row['ID']) for row in UPPER_CASED_ROWS_IN_DATABASE]
    except StartError as e:
        if upper_case_schema_and_field_names:
            assert 'SNOWFLAKE_16' in e.message
        else:
            raise e
    finally:
        snowflake.drop_entities(stage_name=stage_name, schema_name=stage_schema)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
def test_internal_snowflake_user_stage(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    We assert the data from the client to what has been ingested by the Snowflake pipeline.

    Specifically we use Snowflake internal stages and user stage.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'

    # Create a table and stage in Snowflake.
    table = snowflake.create_table(table_name.lower())

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    raw_data = '\n'.join(json.dumps(row) for row in ROWS_IN_DATABASE)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name='~',
                                         table=table_name)

    dev_raw_data_source >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)

    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == [(row['name'], row['id']) for row in ROWS_IN_DATABASE]
    finally:
        snowflake.drop_entities(stage_name='~')
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
def test_CDC_snowflake_user_stage(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    We assert the data from the client to what has been ingested by the Snowflake pipeline.
    The data is in CDC format. To do that:
    - A dev raw data source stage generates the data adding an 'op' field
    - An expression evaluator adds the op field in the header
    - A field remover removes the op field
    - A Snowflake stage adds the data

    We always use the internal user stage

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Create a table and stage in Snowflake.
    table = snowflake.create_table(table_name.lower())

    # Build the pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw Data Source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = ('\n').join((json.dumps(row) for row in CDC_ROWS_IN_DATABASE))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "${record:value('/OP')}"}])

    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/OP']

    # Build Snowflake
    table_key_columns = [{
        "keyColumns": [
            "ID"
        ],
        "table": table_name
    }]
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name='~',
                                         table=table_name,
                                         processing_cdc_data=True)

    # Primary key properties were modified in 1.12
    if Version(snowflake_destination.stage_version) < Version('14'):
        snowflake_destination.set_attributes(tables_key_columns=table_key_columns)
    else:
        snowflake_destination.set_attributes(primary_key_location="TABLE",
                                             table_key_columns=table_key_columns)

    dev_raw_data_source >> expression_evaluator >> field_remover >> snowflake_destination
    pipeline = pipeline_builder.build().configure_for_environment(snowflake)

    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()
        assert data_from_database == [(row['NAME'], row['ID']) for row in CDC_ROWS_IN_DATABASE]
    finally:
        logger.debug('Dropping Snowflake stage ...')
        snowflake.drop_entities(stage_name='~')
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
@sdc_enterprise_lib_min_version({'snowflake': '1.2.0'})
def test_cdc_snowflake_char_type(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    We assert the data from the client to what has been ingested by the Snowflake pipeline.
    The data is in CDC format. To do that:
    - A dev raw data source stage generates the data adding an 'op' field - it includes a char column
    - An expression evaluator adds the op field in the header
    - A field remover removes the op field
    - A Snowflake stage adds the data

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> snowflake_destination
    """

    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Create a table and stage in Snowflake.
    table = sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.CHAR(1))
    )

    # Build the pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    rows_with_char = [
        {'OP': 1, 'ID': 1, 'NAME': '.'},
        {'OP': 1, 'ID': 2, 'NAME': '/'},
        {'OP': 1, 'ID': 3, 'NAME': 'a'}
    ]

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build Dev Raw Data Source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = '\n'.join((json.dumps(row) for row in rows_with_char))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "${record:value('/OP')}"}])

    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/OP']

    # Build Snowflake
    table_key_columns = [{
        "keyColumns": [
            "ID"
        ],
        "table": table_name
    }]
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         processing_cdc_data=True)

    # Primary key properties were modified in 1.12
    if Version(snowflake_destination.stage_version) < Version('14'):
        snowflake_destination.set_attributes(tables_key_columns=table_key_columns)
    else:
        snowflake_destination.set_attributes(primary_key_location="TABLE",
                                             table_key_columns=table_key_columns)

    dev_raw_data_source >> expression_evaluator >> field_remover >> snowflake_destination
    pipeline = pipeline_builder.build().configure_for_environment(snowflake)

    sdc_executor.add_pipeline(pipeline)
    try:
        table.create(engine)
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(table.select())
        data_from_database = result.fetchall()
        result.close()
        assert data_from_database == [(row['ID'], row['NAME']) for row in rows_with_char]
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
def test_special_characters_in_csv_records(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the
    pipeline. After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    We assert the data from the client to what has been ingested by the Snowflake pipeline.

    In this case we verify that we can use different Quoting Modes in order to read CSV with special characters.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    broken_csv = [{'field1': 'abc\,', 'field2': 'xyz,', 'field3': "lmn''", 'field4': 'foo bar'}]

    # Create a table and stage in Snowflake.
    table = sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column('field1', sqlalchemy.String),
        sqlalchemy.Column('field2', sqlalchemy.String),
        sqlalchemy.Column('field3', sqlalchemy.String),
        sqlalchemy.Column('field4', sqlalchemy.String)
    )
    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    raw_data = '\n'.join(json.dumps(row) for row in broken_csv)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         quoting_mode='ESCAPED',
                                         trim_spaces=False)

    dev_raw_data_source >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        table.create(engine)
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == [(row['field1'], row['field2'], row['field3'], row['field4']) for row in
                                      broken_csv]
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_enterprise_lib_min_version({'snowflake': '1.2.0'})
@sdc_min_version('3.7.0')
def test_cdc_snowflake_primary_key_information_from_snowflake(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    After pipeline is run, data is read from Snowflake using Snowflake sqlalchemy client.
    We assert the data from the client to what has been ingested by the Snowflake pipeline.
    The data is in CDC format. To do that:
    - A dev raw data source stage generates the data adding an 'op' field
    - An expression evaluator adds the op field in the header
    - A field remover removes the op field
    - A Snowflake stage adds the data

    In this case we are testing that we are able to get the primary key information directly from snowflake

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # Create a table and stage in Snowflake.
    table = sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column('A', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('B', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('NAME', sqlalchemy.String)
    )

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    raw_data = '\n'.join(json.dumps(row) for row in CDC_ROWS_IN_DATABASE_COMPOSITE_KEY)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "${record:value('/OP')}"}])

    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/OP']

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')

    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         processing_cdc_data=True)

    # Primary key properties were modified in 1.12
    if Version(snowflake_destination.stage_version) < Version('14'):
        snowflake_destination.set_attributes(get_primary_key_information_from_snowflake=True)
    else:
        snowflake_destination.set_attributes(primary_key_location="SNOWFLAKE")

    dev_raw_data_source >> expression_evaluator >> field_remover >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        table.create(engine)
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=300)
        result = engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])  # order by id
        result.close()
        assert data_from_database == sorted([(row['A'], row['B'], row['NAME']) for row in
                                             CDC_ROWS_IN_DATABASE_COMPOSITE_KEY])
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_enterprise_lib_min_version({'snowflake': '1.9.0'})
@pytest.mark.parametrize('pk', [True, False])
def test_cdc_snowflake_primary_key_information_from_snowflake_multithreaded(sdc_builder, sdc_executor, snowflake, pk):
    """Test for Snowflake destination target stage.
    The purpose of this test is to test multithreaded executions with CDC are supported for Snowflake target.
    This test wants to verify that data is shared between threads properly.

    The pipeline look like:
    Snowflake pipeline:
        dev_data_generator >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Create a table and stage in Snowflake.
    table = snowflake.create_table(table_name.lower())
    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build Dev Data Generator
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.fields_to_generate = [
        {'field': 'ID', 'type': 'INTEGER'},
        {'field': 'NAME', 'type': 'POKEMON'}]

    dev_data_generator.set_attributes(delay_between_batches=1000,
                                      batch_size=1,
                                      number_of_threads=3)

    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "1"}])

    # Build Snowflake
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')

    table_key_columns = [{
        "keyColumns": [
            "ID"
        ],
        "table": table_name
    }]
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         processing_cdc_data=True)

    # Primary key properties were modified in 1.12
    if Version(snowflake_destination.stage_version) < Version('14'):
        snowflake_destination.set_attributes(get_primary_key_information_from_snowflake=pk,
                                             tables_key_columns=table_key_columns)
    else:
        snowflake_destination.set_attributes(primary_key_location="SNOWFLAKE" if pk else "TABLE",
                                             table_key_columns=table_key_columns)

    dev_data_generator >> expression_evaluator >> snowflake_destination
    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline=pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', 10, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)
        result = engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()
        assert data_from_database, 'Expected data from snowflake to not be empty'

        pipeline_history = sdc_executor.get_pipeline_history(pipeline)
        records_output_count = pipeline_history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        assert records_output_count <= len(data_from_database), 'Data uploaded to snowflake should be equal or' \
                                                                ' greater than output records'
        records_input_count = pipeline_history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        assert records_input_count <= len(data_from_database), 'Data uploaded to snowflake should be equal or' \
                                                               ' greater than input records'

    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
@sdc_enterprise_lib_min_version({'snowflake': '1.5.0'})
@pytest.mark.parametrize('replicate_decimal_columns', [True, False])
def test_datadrift_decimal_types(sdc_builder, sdc_executor, snowflake, replicate_decimal_columns):
    """Test for Snowflake destination target stage.
    We insert a formal DECIMAL types using dev data generator with different precision and scale and we verify later
    on that the table looks like as we expected.

    The pipeline look like:
    Snowflake pipeline:
        dev_data_generator >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the first pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.fields_to_generate = [
        {'field': 'decimalField', 'precision': 10, 'scale': 2, 'type': 'DECIMAL'},
        {'field': 'decimalField2', 'precision': 38, 'scale': 37, 'type': 'DECIMAL'},
        {'field': 'decimalField3', 'precision': 30, 'scale': 0, 'type': 'DECIMAL'},
        {'field': 'decimalField4', 'precision': 22, 'scale': 127, 'type': 'DECIMAL'},
        {'field': 'decimalField5', 'precision': 100, 'scale': 12, 'type': 'DECIMAL'},
        {'field': 'decimalField6', 'precision': 22, 'scale': 28, 'type': 'DECIMAL'}
    ]

    # Build Snow Flake
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         data_drift_enabled=True,
                                         table_auto_create=True,
                                         replicate_decimal_columns=replicate_decimal_columns)
    dev_data_generator >> snowflake_destination

    # Build first pipeline
    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(100, timeout_sec=300)
    sdc_executor.stop_pipeline(pipeline)

    try:
        table_def = engine.execute(f'describe table "{table_name}"')
        if replicate_decimal_columns:
            assert table_def.fetchall() == [
                ('DECIMALFIELD', 'NUMBER(10,2)', 'COLUMN', 'Y', None, 'N', 'N', None, None, None, None),
                ('DECIMALFIELD2', 'NUMBER(38,37)', 'COLUMN', 'Y', None, 'N', 'N', None, None, None, None),
                ('DECIMALFIELD3', 'NUMBER(30,0)', 'COLUMN', 'Y', None, 'N', 'N', None, None, None, None),
                ('DECIMALFIELD4', 'NUMBER(22,21)', 'COLUMN', 'Y', None, 'N', 'N', None, None, None, None),
                ('DECIMALFIELD5', 'NUMBER(38,12)', 'COLUMN', 'Y', None, 'N', 'N', None, None, None, None),
                ('DECIMALFIELD6', 'NUMBER(22,21)', 'COLUMN', 'Y', None, 'N', 'N', None, None, None, None)]
        else:
            assert table_def.fetchall() == [
                ('DECIMALFIELD', 'FLOAT', 'COLUMN', 'Y', None, 'N', 'N', None, None, None, None),
                ('DECIMALFIELD2', 'FLOAT', 'COLUMN', 'Y', None, 'N', 'N', None, None, None, None),
                ('DECIMALFIELD3', 'FLOAT', 'COLUMN', 'Y', None, 'N', 'N', None, None, None, None),
                ('DECIMALFIELD4', 'FLOAT', 'COLUMN', 'Y', None, 'N', 'N', None, None, None, None),
                ('DECIMALFIELD5', 'FLOAT', 'COLUMN', 'Y', None, 'N', 'N', None, None, None, None),
                ('DECIMALFIELD6', 'FLOAT', 'COLUMN', 'Y', None, 'N', 'N', None, None, None, None)]
        table_def.close()
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)
        engine.execute(f'drop table {table_name}')
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
@sdc_enterprise_lib_min_version({'snowflake': '1.8.0'})
@pytest.mark.parametrize('values_representing_null', [
    [],
    [''],
    ['Rafael Nadal'],
    ['', 'Rafael Nadal'],
    ['NULL', 'null']
])
def test_values_representing_null(sdc_builder, sdc_executor, snowflake, values_representing_null):
    """Test for Snowflake destination target stage when for values_representing_null property. Data is inserted into
    Snowflake with different configurations for this property, making fields get treated as NULL values or not.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    table = snowflake.create_table(table_name.lower())

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    EMPTY_ROWS_IN_DATABASE = [
        {'ID': 1, 'NAME': ''},
        {'ID': 2, 'NAME': 'Rafael Nadal'},
        {'ID': 3, 'NAME': 'Dominic Thiem'}
    ]

    processed_rows_in_database = [(None if row['NAME'] in values_representing_null else row['NAME'], row['ID'])
                                  for row in EMPTY_ROWS_IN_DATABASE]

    raw_data = '\n'.join(json.dumps(row) for row in EMPTY_ROWS_IN_DATABASE)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         values_representing_null=values_representing_null)

    dev_raw_data_source >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert processed_rows_in_database == data_from_database
    finally:
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


DATA_SNOWFLAKE = [
    {"ID": {'uno': 'dos'}},  # MAP
    {"ID": {"a": ["uno", "dos"]}}  # LIST
]


@snowflake
@sdc_min_version('3.7.0')
@sdc_enterprise_lib_min_version({'snowflake': '1.6.0'})
@pytest.mark.parametrize('data', DATA_SNOWFLAKE)
def test_cdc_snowflake_insert_column_variant(sdc_builder, sdc_executor, snowflake, data):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    After pipeline is run, data is read from Snowflake and insert to a VARIANT column. We assert the data
    from the client to what has been ingested by the Snowflake pipeline.
    The data is in CDC format.

    The pipeline looks like:
        dev_raw_data_source  >>  Expression Evaluator >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 20)}'
    engine = snowflake.engine

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.stop_after_first_batch = True
    origin.raw_data = json.dumps(data)

    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.

    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build Expression Evaluator
    expression_evaluator = builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(
        header_attribute_expressions=[{'attributeToSet': 'sdc.operation.type',
                                       'headerAttributeExpression': '4'}])

    table_key_columns=[{
        "keyColumns": [
            "ID"
        ],
        "table": table_name
    }]
    snowflake_destination = builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         processing_cdc_data=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         on_record_error='STOP_PIPELINE')

    # Primary key properties were modified in 1.12
    if Version(snowflake_destination.stage_version) < Version('14'):
        snowflake_destination.set_attributes(tables_key_columns=table_key_columns)
    else:
        snowflake_destination.set_attributes(primary_key_location="TABLE",
                                             table_key_columns=table_key_columns)

    origin >> expression_evaluator >> snowflake_destination

    pipeline = builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table
        rs = engine.execute(f"""
            CREATE TABLE {table_name} (
                "ID" {'VARIANT'} NULL
            )
        """)
        logger.info("Create Table Result")

        # Run pipeline and read from Elasticsearch to assert
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        rs = engine.execute(f'select ID from "{table_name}"')
        rows = [row for row in rs]
        assert len(rows) == 1
        assert json.loads(rows[0][0]) == data['ID']
    finally:
        logger.info('Deleting table with name = %s...', table_name)
        snowflake.drop_entities(stage_name=stage_name)
        snowflake.engine.execute(f'DROP TABLE "{table_name}";')
        snowflake.engine.dispose()


@snowflake
@sdc_min_version('3.22.0')
@sdc_enterprise_lib_min_version({'snowflake': '1.7.0'})
def test_internal_snowflake_tmp_files(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake using the pipeline.
    While pipeline is running, the files created in /tmp must be deleted and not fill up.

    Specifically we use Snowflake internal stages and user stage.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    local_tmp_directory = tempfile.gettempdir()
    file_prefix = f'sdc_{get_random_string(string.ascii_lowercase, 5)}'

    # Create a table and stage in Snowflake.
    table = snowflake.create_table(table_name.lower())

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    raw_data = '\n'.join(json.dumps(row) for row in ROWS_IN_DATABASE)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=False)

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         local_file_prefix=file_prefix,
                                         snowflake_stage_name='~',
                                         table=table_name)

    delay = pipeline_builder.add_stage('Delay')
    # milliseconds to delay between batches, so as we get time to count the files
    delay.set_attributes(delay_between_batches=1000)

    dev_raw_data_source >> delay >> snowflake_destination

    pipeline_snowflake = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline_snowflake)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline_snowflake)

        dir_response = sdc_executor.execute_shell(
            f'find {local_tmp_directory} -type d -name \"{snowflake_destination.local_file_prefix}*\"').stdout.rstrip()
        time.sleep(5)
        count_files_first = int(sdc_executor.execute_shell(f'ls {dir_response}| wc -l').stdout)
        time.sleep(30)
        count_files_second = int(sdc_executor.execute_shell(f'ls {dir_response}| wc -l').stdout)
        sdc_executor.stop_pipeline(pipeline_snowflake)

        assert count_files_first <= 1
        assert count_files_second <= 1

    finally:
        snowflake.drop_entities(stage_name='~')
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_enterprise_lib_min_version({'snowflake': '1.8.0'})
@pytest.mark.parametrize('role', ['PUBLIC', "STF_ROLE"])
def test_snowflake_use_custom_role(sdc_builder, sdc_executor, snowflake, role):
    """Test for Snowflake destination target stage. Data is inserted into Snowflake destination using custom roles .
    Specifically we use Snowflake internal stages and user stage.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    file_prefix = f'sdc_{get_random_string(string.ascii_lowercase, 5)}'

    # Create a table and stage in Snowflake.
    table = snowflake.create_table(table_name.lower())

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    raw_data = '\n'.join(json.dumps(row) for row in ROWS_IN_DATABASE)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         local_file_prefix=file_prefix,
                                         snowflake_stage_name='~',
                                         table=table_name,
                                         use_snowflake_role=True,
                                         snowflake_role_name=role)

    dev_raw_data_source >> snowflake_destination

    pipeline_snowflake = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline_snowflake)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline_snowflake).wait_for_finished()
        result = engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        if role == 'PUBLIC':
            pytest.fail('As role \'PUBLIC\' has no permissions to write to Snowflake, the pipeline should have failed.')
        else:
            assert data_from_database == [(row['name'], row['id']) for row in ROWS_IN_DATABASE]
    except Exception as error:
        if role == 'PUBLIC':
            assert 'SNOWFLAKE_16' in error.message
        else:
            pytest.fail(error.message)
    finally:
        snowflake.drop_entities(stage_name='~')
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_enterprise_lib_min_version({'snowflake': '1.9.0'})
@pytest.mark.parametrize('defaults_from_snowflake_file_format', [True, False])
@pytest.mark.parametrize('file_format', [{'enabled': False, 'options': []},
                                         {'enabled': True, 'options': []},
                                         {'enabled': True, 'options': ['TRIM_SPACE = TRUE']}])
def test_default_from_snowflake_file_format(sdc_builder, sdc_executor, snowflake, defaults_from_snowflake_file_format,
                                            file_format):
    """Test for Snowflake destination target stage when for custom file format and defaults from this file format.
    Tests what happens when this options are combined, as output should be different in each scenario. Testing it
    using trim_space property, setting it to false in SDC and true in snowflake.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    table = snowflake.create_table(table_name.lower())

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Create custom file format if needed
    if file_format['enabled']:
        file_format_name = f'STF_FILE_FORMAT_{get_random_string(string.ascii_uppercase, 5)}'
        snowflake.create_file_format(file_format_name, file_format['options'])
    else:
        file_format_name = ''

    # Process expected data
    if defaults_from_snowflake_file_format and 'TRIM_SPACE = TRUE' in file_format['options']:
        # Manual trim_space for the name field
        expected_data = [(row['name'].lstrip(), row['id']) for row in SPACED_ROWS_IN_DATABASE]
    elif not defaults_from_snowflake_file_format and file_format['enabled']:
        # Due to quoting mode in snowflake format by default (not SDC), we need to prepare data
        expected_data = [("'" + row['name'] + "'", row['id']) for row in SPACED_ROWS_IN_DATABASE]
    else:
        expected_data = [(row['name'], row['id']) for row in SPACED_ROWS_IN_DATABASE]

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    raw_data = '\n'.join(json.dumps(row) for row in SPACED_ROWS_IN_DATABASE)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         trim_spaces=False, # setting this to false to test if this is used or not
                                         defaults_from_snowflake_file_format=defaults_from_snowflake_file_format,
                                         snowflake_file_format=file_format_name)

    dev_raw_data_source >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == expected_data
    finally:
        snowflake.drop_entities(stage_name=stage_name)
        if file_format['enabled']:
            snowflake.drop_entities(file_format_name=file_format_name)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
@sdc_enterprise_lib_min_version({'snowflake': '1.10.0'})
@pytest.mark.parametrize(
    "data_rows, on_error_option, on_error_skip_option, on_error_skip_option_value, expected_data",
    [
        # Shouldn't be differences with the default on_error configuration when there are no errors in the data
        (ROWS_IN_DATABASE, "DEFAULT", None, None, ROWS_IN_DATABASE),
        (ROWS_IN_DATABASE, "CONTINUE", None, None, ROWS_IN_DATABASE),
        (ROWS_IN_DATABASE, "ABORT_STATEMENT", None, None, ROWS_IN_DATABASE),
        (ROWS_IN_DATABASE, "SKIP_FILE", "FIRST", None, ROWS_IN_DATABASE),
        (ROWS_IN_DATABASE, "SKIP_FILE", "NUMBER", 1, ROWS_IN_DATABASE),
        (ROWS_IN_DATABASE, "SKIP_FILE", "PERCENTAGE", 1, ROWS_IN_DATABASE),

        # Test whether the behavior is correct for the different on_error behaviors
        (ROWS_IN_DATABASE_WITH_ERROR, "DEFAULT", None, None, ROWS_IN_DATABASE_WITH_ERROR_ABORT_STATEMENT),
        (ROWS_IN_DATABASE_WITH_ERROR, "CONTINUE", None, None, ROWS_IN_DATABASE_WITH_ERROR_CONTINUE),
        (ROWS_IN_DATABASE_WITH_ERROR, "ABORT_STATEMENT", None, None, ROWS_IN_DATABASE_WITH_ERROR_ABORT_STATEMENT),
        (ROWS_IN_DATABASE_WITH_ERROR, "SKIP_FILE", "FIRST", None, ROWS_IN_DATABASE_WITH_ERROR_ABORT_STATEMENT),
        (ROWS_IN_DATABASE_WITH_ERROR, "SKIP_FILE", "NUMBER", 1, ROWS_IN_DATABASE_WITH_ERROR_ABORT_STATEMENT), # There are two error rows
        (ROWS_IN_DATABASE_WITH_ERROR, "SKIP_FILE", "NUMBER", 2, ROWS_IN_DATABASE_WITH_ERROR_ABORT_STATEMENT), # There are two error rows
        (ROWS_IN_DATABASE_WITH_ERROR, "SKIP_FILE", "NUMBER", 3, ROWS_IN_DATABASE_WITH_ERROR_CONTINUE), # Like continue because there are 2 errors
        (ROWS_IN_DATABASE_WITH_ERROR, "SKIP_FILE", "PERCENTAGE", 20, ROWS_IN_DATABASE_WITH_ERROR_ABORT_STATEMENT),
        (ROWS_IN_DATABASE_WITH_ERROR, "SKIP_FILE", "PERCENTAGE", 40, ROWS_IN_DATABASE_WITH_ERROR_ABORT_STATEMENT),
        (ROWS_IN_DATABASE_WITH_ERROR, "SKIP_FILE", "PERCENTAGE", 60, ROWS_IN_DATABASE_WITH_ERROR_CONTINUE),
    ]
)
def test_on_error_config_stage(sdc_builder, sdc_executor, snowflake, data_rows, on_error_option, on_error_skip_option,
                               on_error_skip_option_value, expected_data):
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # Create a table and stage in Snowflake.
    table = snowflake.create_table(table_name.lower())
    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    raw_data = '\n'.join(json.dumps(row) for row in data_rows)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         data_drift_enabled=False,
                                         ignore_missing_fields=True, # Needed in order to force error in the snowflake end
                                         error_behavior=on_error_option,
                                         skip_file_on_error=on_error_skip_option,
                                         max_error_records=on_error_skip_option_value,
                                         max_error_record_percentage=on_error_skip_option_value)

    dev_raw_data_source >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = snowflake.engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()

        assert data_from_database == [(row['name'], row['id']) for row in expected_data]
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(snowflake.engine)
        snowflake.engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
@sdc_enterprise_lib_min_version({'snowflake': '1.10.0'})
@pytest.mark.parametrize(
    "data_rows, on_error_option, on_error_skip_option, on_error_skip_option_value, expected_data",
    [
        # Shouldn't be differences with the default on_error configuration when there are no errors in the data
        (ROWS_IN_DATABASE, "DEFAULT", None, None, ROWS_IN_DATABASE),
        (ROWS_IN_DATABASE, "CONTINUE", None, None, ROWS_IN_DATABASE),
        (ROWS_IN_DATABASE, "SKIP_FILE", "FIRST", None, ROWS_IN_DATABASE),
        (ROWS_IN_DATABASE, "SKIP_FILE", "NUMBER", 1, ROWS_IN_DATABASE),
        (ROWS_IN_DATABASE, "SKIP_FILE", "PERCENTAGE", 1, ROWS_IN_DATABASE),

        # Test whether the behavior is correct for the different on_error behaviors
        (ROWS_IN_DATABASE_WITH_ERROR, "DEFAULT", None, None, ROWS_IN_DATABASE_WITH_ERROR_ABORT_STATEMENT),
        (ROWS_IN_DATABASE_WITH_ERROR, "CONTINUE", None, None, ROWS_IN_DATABASE_WITH_ERROR_CONTINUE),
        (ROWS_IN_DATABASE_WITH_ERROR, "SKIP_FILE", "FIRST", None, ROWS_IN_DATABASE_WITH_ERROR_ABORT_STATEMENT),
        (ROWS_IN_DATABASE_WITH_ERROR, "SKIP_FILE", "NUMBER", 1, ROWS_IN_DATABASE_WITH_ERROR_ABORT_STATEMENT), # There are two error rows
        (ROWS_IN_DATABASE_WITH_ERROR, "SKIP_FILE", "NUMBER", 2, ROWS_IN_DATABASE_WITH_ERROR_ABORT_STATEMENT), # There are two error rows
        (ROWS_IN_DATABASE_WITH_ERROR, "SKIP_FILE", "NUMBER", 3, ROWS_IN_DATABASE_WITH_ERROR_CONTINUE), # Like continue because there are 2 errors
        (ROWS_IN_DATABASE_WITH_ERROR, "SKIP_FILE", "PERCENTAGE", 20, ROWS_IN_DATABASE_WITH_ERROR_ABORT_STATEMENT),
        (ROWS_IN_DATABASE_WITH_ERROR, "SKIP_FILE", "PERCENTAGE", 40, ROWS_IN_DATABASE_WITH_ERROR_ABORT_STATEMENT),
        (ROWS_IN_DATABASE_WITH_ERROR, "SKIP_FILE", "PERCENTAGE", 60, ROWS_IN_DATABASE_WITH_ERROR_CONTINUE),
    ]
)
def test_on_error_config_snowpipe(sdc_builder, sdc_executor, snowflake, data_rows, on_error_option,
                                  on_error_skip_option, on_error_skip_option_value, expected_data):
    """
    NOTE: When using GCS staging, snowpipe tests are most likely going to fail, due to limitations in Snowflake
    capabilities: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-ts.html#loads-from-google-cloud-storage-delayed-or-files-missed
    So we just skip them. But if there is the need of trying Snowpipe+gcs, they can eventually pass.
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'
    pipe_name = f'STF_PIPE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Create a table, stage and pipe in Snowflake.
    table = snowflake.create_table(table_name.lower())
    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)
    snowflake.create_pipe(pipe_name, stage_name, table_name)

    # Build the pipeline with created entities in Snowflake stage configurations.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    raw_data = '\n'.join(json.dumps(row) for row in data_rows)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(pipe=pipe_name,
                                         purge_stage_file_after_ingesting=False,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         use_snowpipe=True,
                                         data_drift_enabled=False,
                                         ignore_missing_fields=True, # Needed in order to force error in the snowflake end
                                         snowpipe_auto_create=True, # The on_error_behavior option only appears when auto_create is enabled
                                         snowpipe_error_behavior=on_error_option,
                                         snowpipe_skip_file_on_error=on_error_skip_option,
                                         snowpipe_max_error_records=on_error_skip_option_value,
                                         snowpipe_max_error_record_percentage=on_error_skip_option_value)

    dev_raw_data_source >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        def query_function():
            return engine.execute(table.select())

        data_from_database = wait_for_snowpipe_data_ingestion(query_function, 5 if len(expected_data) > 0 else 1)
        assert len(data_from_database) == len(expected_data), \
            "The expected data length should be the same as the data found in the database"
        assert data_from_database == [(row['name'], row['id']) for row in expected_data]

    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(pipe_name=pipe_name, stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('4.1.0')
@pytest.mark.parametrize('number_of_threads_and_tables', [2, 8])
@pytest.mark.parametrize('number_of_records', [100_000])
@pytest.mark.parametrize('batch_size', [10_000])
def test_multithreaded_multiple_tables(sdc_builder, sdc_executor, snowflake, number_of_threads_and_tables,
                                       number_of_records, batch_size):
    """Snowflake destination with different thread and tables combinations. To achieve this, we create
    a long sequence with dev_data_generator which will be used to differentiate the table the record will go to.
    Very similar to benchmark test, but ensuring data is correct as well"""

    # We generate the number of tables we need
    random_table_suffix = get_random_string(string.ascii_uppercase, 5)
    table_names = [f'STF_TABLE_{idx}_{random_table_suffix}' for idx in range(number_of_threads_and_tables)]
    tables = [snowflake.create_table(table_name) for table_name in table_names]

    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'
    # The following is a path inside a bucket in the case of AWS S3 or
    # a path inside a container in the case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(
        records_to_be_generated=number_of_records, batch_size=batch_size,
        number_of_threads=number_of_threads_and_tables, delay_between_batches=10,
        fields_to_generate=[{"type": "LONG_SEQUENCE", "field": "ID"}, {"type": "POKEMON", "field": "NAME"}]
    )

    # We need to deduplicate records as dev_generator might create the same record in multiple threads
    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    record_deduplicator.set_attributes(
        compare="SPECIFIED_FIELDS", fields_to_compare=['/ID']
    )
    trash = pipeline_builder.add_stage('Trash')

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         data_drift_enabled=True,
                                         ignore_missing_fields=True,
                                         table="STF_TABLE_${record:value('/ID') % " + str(number_of_threads_and_tables)
                                               + '}_' + random_table_suffix,
                                         connection_pool_size=number_of_threads_and_tables)

    wiretap = pipeline_builder.add_wiretap()

    dev_data_generator >> record_deduplicator >> [snowflake_destination, wiretap.destination]
    record_deduplicator >> trash

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=1200)

        # We collect the data received in wiretap and create an array for each table
        expected_data_per_table = {table_name: [] for table_name in table_names}
        for record in wiretap.output_records:
            record_table_name = \
                f'STF_TABLE_{int(str(record.field["ID"])) % number_of_threads_and_tables}_{random_table_suffix}'
            expected_data_per_table[record_table_name].append((record.field['NAME'], record.field['ID']))

        # And then for each table, we check what we received in wiretap matches
        for table in tables:
            result = snowflake.engine.execute(table.select())
            data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
            result.close()
            # Order by id per table to compare as well
            sorted_expected_data = sorted(expected_data_per_table[table.name],
                                          key=lambda x: (int(str(x[1])), str(x[0])))
            # And compare sorted data per table
            assert data_from_database == sorted_expected_data, 'Data read from Snowflake database should have been' \
                                                               'the same as the data captured in wiretap.'
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        [table.drop(engine) for table in tables]
        engine.dispose()


@snowflake
@sdc_min_version('3.7.0')
@sdc_enterprise_lib_min_version({'snowflake': '1.10.0'})
def test_aws_configuration_values(sdc_builder, sdc_executor, snowflake):
    """Test for Snowflake destination target stage.
    We stress the maximum values for AWS timeouts and error retries

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # Create a table and stage in Snowflake.
    table = snowflake.create_table(table_name.lower())
    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location='AWS_S3')

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    raw_data = '\n'.join(json.dumps(row) for row in ROWS_IN_DATABASE)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(stage_location='AWS_S3',
                                         purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         s3_connection_timeout=600,
                                         s3_socket_timeout=600,
                                         s3_max_error_retry=50)

    dev_raw_data_source >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(table.select())
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == [(row['name'], row['id']) for row in ROWS_IN_DATABASE]
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


def wait_for_snowpipe_data_ingestion(query_function, max_attempts=5):
    attempts = 0
    data_from_database = []
    while not data_from_database and attempts < max_attempts:
        try:
            logger.info('Waiting for a minute to let time for data ingestion using Snowpipe ...')
            time.sleep(60)
            attempts += 1
            result = query_function()
            data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
            result.close()
        except Exception as e:
            logger.info(f'Found an error while querying database: {e}. Ignoring...')
    return data_from_database
