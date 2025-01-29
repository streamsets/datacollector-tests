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
import os
import string
import tempfile
import textwrap

import pytest
import sqlalchemy
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import snowflake, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [snowflake, sdc_min_version('5.6.0')]

# from CommonDatabaseHeader.java
PRIMARY_KEY_COLUMN_OLD_VALUE = 'jdbc.primaryKey.before'
PRIMARY_KEY_COLUMN_NEW_VALUE = 'jdbc.primaryKey.after'
PRIMARY_KEY_SPECIFICATION = 'jdbc.primaryKeySpecification'

# from OperationType.java
CDC_OPERATIONS = {
    "INSERT": 1,
    "DELETE": 2,
    "UPDATE": 3,
    "UPSERT": 4,
    "UNSUPPORTED": 5,
    "UNDELETE": 6,
    "REPLACE": 7,
    "MERGE": 8,
    "LOAD": 9
}

# AWS S3 bucket in case of AWS or Azure blob storage container in case of Azure.
STORAGE_BUCKET_CONTAINER = 'snowflake'

NEW_STAGE_LOCATIONS = {'AZURE': 'BLOB_STORAGE'}


def get_stage_location(sdc_builder, stage_location):
    if Version(sdc_builder.version) < Version("5.7.0"):
        return stage_location
    else:
        new_stage_location = NEW_STAGE_LOCATIONS.get(stage_location)
        return new_stage_location if new_stage_location else stage_location


CDC_TEST_CASES = [
    (
        ['col_1'],
        [],
        [
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 1}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 1, 'col_2': 2}}
        ],
        [
            {'col_1': 1, 'col_2': 2}
        ]
    ),
    (
        ['col_1'],
        [
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 1}}
        ],
        [  # if the 3 operations are executed in the same batch, table doesn't even get created
            {'OP': 'UPDATE', 'ROWS': {'col_1': 2, 'col_2': 2}},
            {'OP': 'DELETE', 'ROWS': {'col_1': 2, 'col_2': 2}}
        ],
        []
    ),
    (
        ['col_1'],
        [
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 1}}
        ],
        [
            {'OP': 'MERGE', 'ROWS': {'col_1': 1, 'col_2': 2}}
        ],
        [
            {'col_1': 1, 'col_2': 1}  # we will just have the INSERT record in the db
        ]
    ),
    (
        ['col_1'],
        [],
        [
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 1}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 1, 'col_2': 2}},
            {'OP': 'REPLACE', 'ROWS': {'col_1': 1, 'col_2': 3}},
            {'OP': 'UPSERT', 'ROWS': {'col_1': 1, 'col_2': 4}}
        ],
        [
            {'col_1': 1, 'col_2': 4}
        ]
    ),
    (
        ['col_1'],
        [
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 1}}
        ],
        [
            {'OP': 'UPDATE', 'ROWS': {'col_1': 2, 'col_2': 1}},
            {'OP': 'DELETE', 'ROWS': {'col_1': 2, 'col_2': 1}},
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 3}},
            {'OP': 'DELETE', 'ROWS': {'col_1': 1, 'col_2': 4}},
            {'OP': 'INSERT', 'ROWS': {'col_1': 5, 'col_2': 5}}
        ],
        [
            {'col_1': 5, 'col_2': 5}
        ]
    ),
    (
        ['col_1'],
        [
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 1}}
        ],
        [
            {'OP': 'UPDATE', 'ROWS': {'col_1': 2, 'col_2': 1}},
            {'OP': 'DELETE', 'ROWS': {'col_1': 2, 'col_2': 1}},
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 3}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 3, 'col_2': 4}},
            {'OP': 'DELETE', 'ROWS': {'col_1': 3, 'col_2': 4}},
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 5}}
        ],
        [
            {'col_1': 1, 'col_2': 5}
        ]
    ),
    (
        ['col_1', 'col_2'],
        [
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 1}}
        ],
        [
            {'OP': 'UPDATE', 'ROWS': {'col_1': 1, 'col_2': 2}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 2, 'col_2': 2}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 2, 'col_2': 3}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 3, 'col_2': 3}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 4, 'col_2': 4}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 4, 'col_2': 5}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 5, 'col_2': 5}}
        ],
        [
            {'col_1': 5, 'col_2': 5}
        ]
    ),
    (
        ['col_1', 'col_2'],
        [
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 1}}
        ],
        [
            {'OP': 'UPDATE', 'ROWS': {'col_1': 1, 'col_2': 2}},
            {'OP': 'DELETE', 'ROWS': {'col_1': 1, 'col_2': 2}},
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 2}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 1, 'col_2': 1}},
            {'OP': 'DELETE', 'ROWS': {'col_1': 1, 'col_2': 1}},
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 1}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 1, 'col_2': 2}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 2, 'col_2': 2}}
        ],
        [
            {'col_1': 2, 'col_2': 2}
        ]
    ),
    (
        ['col_1', 'col_2'],
        [],
        [
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 1}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 2, 'col_2': 2}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 3, 'col_2': 3}},
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 1}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 5, 'col_2': 5}}
        ],
        [
            {'col_1': 3, 'col_2': 3}, {'col_1': 5, 'col_2': 5}
        ]
    ),
    (
        ['col_1', 'col_2'],
        [
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 1}}
        ],
        [
            {'OP': 'DELETE', 'ROWS': {'col_1': 1, 'col_2': 1}},
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 2}},
            {'OP': 'DELETE', 'ROWS': {'col_1': 1, 'col_2': 2}},
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 1}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 2, 'col_2': 2}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 3, 'col_2': 3}},
            {'OP': 'UPDATE', 'ROWS': {'col_1': 4, 'col_2': 4}},
            {'OP': 'DELETE', 'ROWS': {'col_1': 4, 'col_2': 4}},
            {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 1}}
        ],
        [
            {'col_1': 1, 'col_2': 1}
        ]
    ),
]


@pytest.mark.parametrize('primary_key_location', ['HEADER', 'TABLE'])
@pytest.mark.parametrize('pk_columns,'  # pk columns used by the CDC process
                         'ops_1,'  # ops_1: used to set the state of the db, through the same pipeline. It can be empty
                         'ops_2,'  # ops_2: used test the consolidation process, and it is required in the test
                         'expected_db_rows',  # it can be empty if the final state is empty, but table must exist
                         CDC_TEST_CASES, ids=['-'.join([op['OP'] for op in case[2]]) for case in CDC_TEST_CASES])
def test_cdc_record_consolidator(sdc_builder, sdc_executor, snowflake, primary_key_location, pk_columns, ops_1, ops_2,
                                 expected_db_rows):
    """
    This test has all the cases for the different operations sets contained in CDCRecordConsolidator.java.
    Different behavior is found if records appear in different batches. To simulate this behavior, we use
    2 pipelines. Consolidation process is stateless from batch to batch, so it's the same thing: it just
    depends on initial db state and the headers.
    Table is created by any pipeline that gets executed first, either the one setting up initial state or the
    one already in charge of the actual test.
    We test it for Snowflake destination stage, but other enterprises also use this class.

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    records, pk_header_attribute_expressions = generate_cdc_records_with_header(pk_columns, ops_1 + ops_2)
    records_1 = records[:len(ops_1)]
    records_2 = records[len(ops_1):]
    try:
        if len(ops_1) > 0:
            pipeline_builder = sdc_builder.get_pipeline_builder()
            # Get CDC origin simulator for given operations (batch 1)
            pipeline = generate_snowflake_cdc_pipeline(pipeline_builder, snowflake, stage_name, table_name,
                                                       primary_key_location, pk_columns, records_1,
                                                       pk_header_attribute_expressions)
            sdc_executor.add_pipeline(pipeline)
            sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        assert len(ops_2) > 0
        pipeline_builder = sdc_builder.get_pipeline_builder()
        # Get CDC origin simulator for given operations (batch 2)
        pipeline_2 = generate_snowflake_cdc_pipeline(pipeline_builder, snowflake, stage_name, table_name,
                                                     primary_key_location, pk_columns, records_2,
                                                     pk_header_attribute_expressions)
        sdc_executor.add_pipeline(pipeline_2)
        sdc_executor.start_pipeline(pipeline=pipeline_2).wait_for_finished()

        # And assert the results are correct
        result = engine.execute(f'SELECT * FROM "{table_name}";')
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()
        expected_data = sorted(tuple(v for k, v in row.items()) for row in expected_db_rows)
        assert data_from_database == expected_data
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)
        engine.execute(f'DROP TABLE {table_name}')
        engine.dispose()


@pytest.mark.parametrize('pk_columns,'
                         'ops',
                         [
                             (
                                 ['col_1'],
                                 [
                                     {'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 1}},
                                     {'OP': 'UPDATE', 'ROWS': {'col_1': 2, 'col_2': 2}},
                                     {'OP': 'DELETE', 'ROWS': {'col_1': 2, 'col_2': 2}}
                                 ]
                             )
                         ], ids=['INSERT-UPDATE-DELETE'])
def test_cdc_snowflake_empty_result_table_not_created(sdc_builder, sdc_executor, snowflake, pk_columns, ops):
    """
    Similar to the test above, but ensuring the table doesn't get created if consolidation result is 0 records

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> snowflake_destination
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    records, pk_header_attribute_expressions = generate_cdc_records_with_header(pk_columns, ops)
    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()
        # Get CDC origin simulator for given operations
        pipeline = generate_snowflake_cdc_pipeline(pipeline_builder, snowflake, stage_name, table_name, 'HEADER',
                                                   pk_columns, records, pk_header_attribute_expressions)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        # And assert the results are correct
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('stage.Snowflake_01.errorRecords.counter').count == 0
        engine.execute(f'SELECT * FROM "{table_name}";')
    except sqlalchemy.exc.ProgrammingError as e:
        # we should not be able to SELECT the table as it shouldn't exist
        assert f"Object '{table_name.upper()}' does not exist or not authorized." in str(e)
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)
        try:
            engine.execute(f'DROP TABLE {table_name}')  # just in case
        except Exception:
            pass
        engine.dispose()


@pytest.mark.parametrize('stage_location', ["INTERNAL", "AWS_S3", "AZURE", "GCS"])
@pytest.mark.parametrize('pk_columns,'
                         'ops,'
                         'expected_db_rows',
                         [
                             (
                                 ['col_1'],
                                 [
                                     {'TABLE': 0, 'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 'Rogelio Federer'}},
                                     {'TABLE': 1, 'OP': 'INSERT', 'ROWS': {'col_1': 2, 'col_2': 'Rafa Nadal'}},
                                     {'TABLE': 0, 'OP': 'INSERT', 'ROWS': {'col_1': 3, 'col_2': 'Domi Thiem'}},
                                     {'TABLE': 1, 'OP': 'INSERT', 'ROWS': {'col_1': 4, 'col_2': 'Juan Del Potro'}},
                                     {'TABLE': 0, 'OP': 'UPDATE', 'ROWS': {'col_1': 1, 'col_2': 'Roger Federer'}},
                                     {'TABLE': 1, 'OP': 'UPDATE', 'ROWS': {'col_1': 2, 'col_2': 'Rafael Nadal'}},
                                     {'TABLE': 0, 'OP': 'UPDATE', 'ROWS': {'col_1': 3, 'col_2': 'Dominic Thiem'}},
                                     {'TABLE': 1, 'OP': 'DELETE', 'ROWS': {'col_1': 4, 'col_2': 'Juan Del Potro'}}
                                 ],
                                 [
                                     [{'col_1': 1, 'col_2': 'Roger Federer'}, {'col_1': 3, 'col_2': 'Dominic Thiem'}],
                                     [{'col_1': 2, 'col_2': 'Rafael Nadal'}]]
                             )
                         ], ids=['multiple-ops-and-tables'])
def test_cdc_snowflake_multiple_tables_and_ops(sdc_builder, sdc_executor, snowflake, stage_location, pk_columns, ops,
                                               expected_db_rows):
    """
    Similar to test above, but we make sure operations for different tables are properly handled

    The pipeline looks like:
    Snowflake pipeline:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> snowflake_destination
    """
    stage_location = get_stage_location(sdc_builder, stage_location)
    table_name_1 = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    table_name_2 = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    table_names = [table_name_1, table_name_2]
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location=stage_location)

    records, pk_header_attribute_expressions = generate_cdc_records_with_header(pk_columns, ops)
    for record, op in zip(records, ops):
        record['TABLE'] = table_names[op['TABLE']]
        record['PK_UPDATE'] = {}

    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()
        # Get CDC origin simulator for given operations
        pipeline = generate_snowflake_cdc_pipeline(pipeline_builder, snowflake, stage_name, "${record:value('/TABLE')}",
                                                   'HEADER', pk_columns, records, pk_header_attribute_expressions,
                                                   stage_location=stage_location, column_fields_to_ignore='TABLE')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        # And assert the results are correct
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('stage.Snowflake_01.errorRecords.counter').count == 0

        result = engine.execute(f'SELECT * FROM "{table_name_1}";')
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()

        expected_data_0 = sorted(tuple(v for k, v in row.items()) for row in expected_db_rows[0])
        assert sorted(data_from_database) == expected_data_0

        result = engine.execute(f'SELECT * FROM "{table_name_2}";')
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()

        expected_data_1 = sorted(tuple(v for k, v in row.items()) for row in expected_db_rows[1])
        assert sorted(data_from_database) == expected_data_1
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', stage_name)
        snowflake.drop_entities(stage_name=stage_name)
        engine.execute(f'DROP TABLE {table_name_1}')
        engine.execute(f'DROP TABLE {table_name_2}')
        engine.dispose()


@sdc_min_version('6.2.0')
@pytest.mark.parametrize('use_upper_case', [False, True])
@pytest.mark.parametrize('pk_columns,'
                         'ops,'
                         'expected_db_rows',
                         [
                             (
                                 ['col_1'],
                                 [
                                     {'TABLE': 0, 'OP': 'INSERT', 'ROWS': {'col_1': 1, 'col_2': 'Rogelio Federer'}},
                                     {'TABLE': 1, 'OP': 'INSERT', 'ROWS': {'col_1': 2, 'col_2': 'Rafa Nadal'}},
                                     {'TABLE': 0, 'OP': 'INSERT', 'ROWS': {'col_1': 3, 'col_2': 'Domi Thiem'}},
                                     {'TABLE': 1, 'OP': 'INSERT', 'ROWS': {'col_1': 4, 'col_2': 'Juan Del Potro'}},
                                     {'TABLE': 0, 'OP': 'UPDATE', 'ROWS': {'col_1': 1, 'col_2': 'Roger Federer'}},
                                     {'TABLE': 1, 'OP': 'UPDATE', 'ROWS': {'col_1': 2, 'col_2': 'Rafael Nadal'}},
                                     {'TABLE': 0, 'OP': 'UPDATE', 'ROWS': {'col_1': 3, 'col_2': 'Dominic Thiem'}},
                                     {'TABLE': 1, 'OP': 'DELETE', 'ROWS': {'col_1': 4, 'col_2': 'Juan Del Potro'}}
                                 ],
                                 [
                                     [{'col_1': 1, 'col_2': 'Roger Federer'}, {'col_1': 3, 'col_2': 'Dominic Thiem'}],
                                     [{'col_1': 2, 'col_2': 'Rafael Nadal'}]]
                             )
                         ], ids=['multiple-ops-and-tables'])
def test_cdc_snowflake_multiple_tables_and_ops_using_snowflake_pk_location(
        sdc_builder, sdc_executor,
        snowflake, use_upper_case, pk_columns, ops, expected_db_rows):
    """
    Similar to test above, but we make sure operations for different tables are properly handled

    The pipeline looks like:
    Snowflake pipeline:
        Directory source  >>  Expression Evaluator >> Field Remover >> snowflake_destination
    """
    stage_location = get_stage_location(sdc_builder, "INTERNAL")
    table_name_1 = f'STF_TABLE_{get_random_string(string.ascii_lowercase, 5)}'
    table_name_2 = f'STF_TABLE_{get_random_string(string.ascii_lowercase, 5)}'
    table_names = [table_name_1, table_name_2]
    create_table_1_query = f'CREATE TABLE "{table_name_1}" ("col_1" NUMBER, "col_2" STRING, primary key ("col_1"));'
    create_table_2_query = f'CREATE TABLE "{table_name_2}" ("col_1" NUMBER, "col_2" STRING, primary key ("col_1"));'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_lowercase, 5)}'
    formatted_stage_name = stage_name
    formatted_table_name_1 = table_name_1
    formatted_table_name_2 = table_name_2
    if use_upper_case:
        formatted_stage_name = stage_name.upper()
        create_table_1_query = create_table_1_query.upper()
        create_table_2_query = create_table_2_query.upper()
        formatted_table_name_1 = table_name_1.upper()
        formatted_table_name_2 = table_name_2.upper()
    engine = snowflake.engine

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(formatted_stage_name, storage_path, stage_location=stage_location)

    records, pk_header_attribute_expressions = generate_cdc_records_with_header(pk_columns, ops)
    for record, op in zip(records, ops):
        record['TABLE'] = table_names[op['TABLE']]
        record['PK_UPDATE'] = {}

    file_directory: str = os.path.join(tempfile.gettempdir(), get_random_string())

    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()

        sdc_executor.execute_shell(f'mkdir -m 777 {file_directory}')
        file_name = f'{get_random_string()}.txt'

        # Get CDC origin simulator for given operations
        pipeline = generate_snowflake_cdc_pipeline(
            pipeline_builder, snowflake, stage_name, "${record:value('/TABLE')}",
            'SNOWFLAKE', pk_columns, records, pk_header_attribute_expressions,
            stage_location=stage_location, column_fields_to_ignore='TABLE',
            table_auto_create=False, data_drift_enabled=False,
            use_upper_case=use_upper_case,
            pipeline_source=generate_directory_source(pipeline_builder,
                                                      input_file_directory=file_directory,
                                                      input_file_name=file_name)
        )
        sdc_executor.add_pipeline(pipeline)

        pipeline_cmd = sdc_executor.start_pipeline(pipeline=pipeline)
        pipeline_cmd.wait_for_status(status='RUNNING', timeout_sec=30)

        logger.debug(f"Executing query: {create_table_1_query}")
        snowflake.engine.execute(create_table_1_query)
        logger.debug(f"Executing query: {create_table_2_query}")
        snowflake.engine.execute(create_table_2_query)
        logger.debug("Sending records to Directory origin...")
        file_content = '\\n'.join(json.dumps(record) for record in records)
        file_content = file_content.replace("\"", "\\\"")
        file_writer(sdc_executor, os.path.join(file_directory, file_name), file_content)

        pipeline_cmd.wait_for_pipeline_output_records_count(8, timeout_sec=30)
        sdc_executor.stop_pipeline(pipeline=pipeline)

        # And assert the results are correct
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('stage.Snowflake_01.errorRecords.counter').count == 0

        result = engine.execute(f'SELECT * FROM "{formatted_table_name_1}";')
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()

        expected_data_0 = sorted(tuple(v for k, v in row.items()) for row in expected_db_rows[0])
        assert sorted(data_from_database) == expected_data_0

        result = engine.execute(f'SELECT * FROM "{formatted_table_name_2}";')
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()

        expected_data_1 = sorted(tuple(v for k, v in row.items()) for row in expected_db_rows[1])
        assert sorted(data_from_database) == expected_data_1
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        logger.debug('Dropping Snowflake stage %s ...', formatted_stage_name)
        snowflake.drop_entities(stage_name=formatted_stage_name)
        engine.execute(f'DROP TABLE "{formatted_table_name_1}"')
        engine.execute(f'DROP TABLE "{formatted_table_name_2}"')
        engine.dispose()
        sdc_executor.execute_shell(f'rm -rf {file_directory}')


def generate_cdc_records_with_header(pk_columns, ops):
    """
        Generates records and headers from the operations defined.
    """
    records = []
    pk_header_attribute_expressions = []

    pk_definition_header = {}
    for pk in pk_columns:
        pk_definition_header[pk] = {}  # leave it empty, we do not use the content

        pk_header_attribute_expressions.append({
            'attributeToSet': f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.{pk}',
            'headerAttributeExpression':
                f"${{record:value('/PK_UPDATE/{PRIMARY_KEY_COLUMN_OLD_VALUE}.{pk}')}}"
        })
        pk_header_attribute_expressions.append({
            'attributeToSet': f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.{pk}',
            'headerAttributeExpression':
                f"${{record:value('/PK_UPDATE/{PRIMARY_KEY_COLUMN_NEW_VALUE}.{pk}')}}"
        })

    prev_op = {}
    for op in ops:
        pk_update_definition_header = {}

        if is_pk_column_update(pk_columns, op, prev_op):
            for pk in pk_columns:
                pk_update_definition_header[f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.{pk}'] = prev_op.get('ROWS').get(pk)
                pk_update_definition_header[f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.{pk}'] = op['ROWS'].get(pk)

        prev_op = op

        records.append({
            'OP': CDC_OPERATIONS.get(op['OP']),
            'PK_DEF': json.dumps(pk_definition_header),
            'PK_UPDATE': pk_update_definition_header,
            **op['ROWS']
        })

    return records, pk_header_attribute_expressions


def is_pk_column_update(pk_columns, op, prev_op):
    for pk in pk_columns:
        # if any pk column is different, we need every pk column in the header
        if prev_op and op['ROWS'].get(pk) != prev_op.get('ROWS').get(pk):
            return True
    return False


def generate_snowflake_cdc_pipeline(pipeline_builder, snowflake, stage_name, table_name, primary_key_location,
                                    pk_columns, records, pk_header_attribute_expressions, stage_location='INTERNAL',
                                    column_fields_to_ignore='', table_auto_create=True, data_drift_enabled=True,
                                    pipeline_source=None, use_upper_case=True):
    """
        Generates pipeline with the records and headers within the stages used, from the operations we want to test.
    """
    # Build Dev Raw Data Source
    if pipeline_source is None:
        raw_data = '\n'.join(json.dumps(record) for record in records)
        pipeline_source = pipeline_builder.add_stage('Dev Raw Data Source')
        pipeline_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    header_attribute_expressions = [
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': "${record:value('/OP')}"},
        {'attributeToSet': PRIMARY_KEY_SPECIFICATION,
         'headerAttributeExpression': "${record:value('/PK_DEF')}"}
    ]
    header_attribute_expressions.extend(pk_header_attribute_expressions)

    # Build Expression Evaluator
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=header_attribute_expressions)

    # Build Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ['/OP', '/PK_DEF', '/PK_UPDATE']

    # Build Snowflake
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')

    snowflake_destination.set_attributes(stage_location=stage_location,
                                         purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         column_fields_to_ignore=column_fields_to_ignore,
                                         processing_cdc_data=True,
                                         table_auto_create=table_auto_create,
                                         primary_key_location=primary_key_location,
                                         data_drift_enabled=data_drift_enabled)

    snowflake_destination.configuration.update({
        "config.data.upperCaseSchema": use_upper_case
    })

    if primary_key_location == 'TABLE':
        snowflake_destination.set_attributes(table_key_columns=[{
                                                 "keyColumns": pk_columns,
                                                 "table": table_name
                                             }])

    pipeline_source >> expression_evaluator >> field_remover >> snowflake_destination

    return pipeline_builder.build().configure_for_environment(snowflake)


def generate_directory_source(pipeline_builder, input_file_directory='', input_file_name=''):
    pipeline_source = pipeline_builder.add_stage('Directory')
    pipeline_source.set_attributes(
        files_directory=input_file_directory,
        file_name_pattern=input_file_name,
        data_format='JSON',
    )

    return pipeline_source


def file_writer(sdc_executor, file_path, file_contents):
    encoding = 'utf8'
    file_writer_script_binary = """
        with open('{filepath}', 'wt') as f:
            f.write('{file_contents}')
    """

    builder = sdc_executor.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='noop', stop_after_first_batch=True)
    jython_evaluator = builder.add_stage('Jython Evaluator')

    file_writer_script = file_writer_script_binary
    jython_evaluator.script = textwrap.dedent(file_writer_script).format(filepath=str(file_path),
                                                                         file_contents=file_contents,
                                                                         encoding=encoding)
    trash = builder.add_stage('Trash')
    dev_raw_data_source >> jython_evaluator >> trash
    pipeline = builder.build('File writer pipeline')

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    sdc_executor.remove_pipeline(pipeline)
