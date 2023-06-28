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
import pytest
import random
import re
import sqlalchemy
import string
import tempfile

from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version
from time import sleep
from .utils.utils_azure_synapse import \
    CDC_PK_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY_HEADER,\
    CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY,\
    EXPECTED_DRIFT_RESULT,\
    EXPECTED_DRIFT_RESULT_WITH_INVALID_TYPES_ROWS,\
    EXTENDED_ROWS_IN_DATABASE,\
    INVALID_TYPES_ROWS_IN_DATABASE,\
    PRIMARY_KEY_COLUMN_NEW_VALUE,\
    PRIMARY_KEY_SPECIFICATION,\
    PRIMARY_KEY_COLUMN_OLD_VALUE,\
    ROWS_IN_DATABASE,\
    ROWS_IN_DATABASE_WITH_PARTITION,\
    STAGE_NAME,\
    clean_up,\
    delete_schema,\
    delete_table,\
    get_synapse_pipeline,\
    stop_pipeline

pytestmark = [azure('synapse'), sdc_min_version('5.5.0')]

logger = logging.getLogger(__name__)


@pytest.mark.parametrize('create_new_columns_as_varchar', [True, False])
def test_synapse_destination_data_drift(sdc_builder, sdc_executor, azure, create_new_columns_as_varchar):
    """
    Test for Azure Synapse target stage. We send an initial batch of records to the destination,
    then send a second batch with extra fields. With data drift enabled this should create the new columns in
    the database and set the value to NULL for the previously inserted records.

    The pipelines look like:
    dev_raw_data_source >> azure_synapse_destination
    """
    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'

    synapse_pipeline = get_synapse_pipeline(
        sdc_builder,
        azure,
        ROWS_IN_DATABASE,
        stage_file_prefix,
        destination_table_name
    )
    synapse_warehouse_pipeline = get_synapse_pipeline(
        sdc_builder,
        azure,
        EXTENDED_ROWS_IN_DATABASE,
        stage_file_prefix,
        destination_table_name
    )
    synapse_warehouse_pipeline.stages[1].set_attributes(
        create_new_columns_as_varchar=create_new_columns_as_varchar
    )

    sdc_executor.add_pipeline(synapse_pipeline)
    sdc_executor.add_pipeline(synapse_warehouse_pipeline)
    engine = azure.synapse.engine
    try:
        # Run regular pipeline to generate initial records
        sdc_executor.start_pipeline(synapse_pipeline).wait_for_finished()

        # Run drift pipeline with the extra country column
        sdc_executor.start_pipeline(synapse_warehouse_pipeline).wait_for_finished()

        stmt = f'select * from {azure.synapse_database_schema}.{destination_table_name}'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        assert data_from_database == [
            (
                row['id'],
                row['name'],
                row['country'],
                str(row['birth']) if row['birth'] and create_new_columns_as_varchar else row['birth']
            ) for row in EXPECTED_DRIFT_RESULT
        ]

        stmt = (
            f' select col.name as column_name, t.name as data_type'
            f' from sys.tables as tab'
            f' inner join sys.columns as col on tab.object_id = col.object_id'
            f' left join sys.types as t on col.user_type_id = t.user_type_id'
            f' where tab.name=\'{destination_table_name}\''
            f' and (col.name=\'BIRTH\' or col.name=\'COUNTRY\') order by col.name;'
        )
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()
        if create_new_columns_as_varchar:
            assert data_from_database == [('BIRTH', 'varchar'), ('COUNTRY', 'varchar')]
        else:
            assert data_from_database == [('BIRTH', 'int'), ('COUNTRY', 'varchar')]

    finally:
        try:
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, synapse_pipeline)
            stop_pipeline(sdc_executor, synapse_warehouse_pipeline)
        except Exception as ex:
            logger.error(ex)


@pytest.mark.parametrize('create_new_columns_as_varchar', [True, False])
@pytest.mark.parametrize('case_insensitive', [True, False])
@pytest.mark.parametrize('table_name_uppercase', [True, False])
def test_synapse_destination_data_drift_same_pipeline(
        sdc_builder,
        sdc_executor,
        azure,
        create_new_columns_as_varchar,
        case_insensitive,
        table_name_uppercase
):
    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    if table_name_uppercase:
        destination_table_name = destination_table_name.upper()
    logger.info('destination_table_name = %s', destination_table_name)
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'

    builder = sdc_builder.get_pipeline_builder()
    directory_origin = builder.add_stage('Directory')
    directory_origin.set_attributes(
        files_directory='/tmp',
        file_name_pattern=f'stf_*_{destination_table_name}_*.data',
        data_format='JSON',
        batch_size_in_recs=1
    )

    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        auto_create_table=True,
        table=destination_table_name,
        enable_data_drift=True,
        ignore_missing_fields=True,
        ignore_fields_with_invalid_types=True,
        purge_stage_file_after_loading=True,
        stage_file_prefix=stage_file_prefix,
        directory_template='${YYYY()}-${MM()}-${DD()}-${hh()}',
        create_new_columns_as_varchar=create_new_columns_as_varchar,
        case_insensitive=case_insensitive
    )

    directory_origin >> azure_synapse_destination

    synapse_pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(synapse_pipeline)

    try:

        # Create first file with first record
        builder_file_1 = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder_file_1.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(
            data_format='JSON',
            raw_data='{"NAME": "Mark Brooks", "ADDRESS": "123 Maple Lane"}',
            stop_after_first_batch=True
        )
        local_fs_file_1 = builder_file_1.add_stage('Local FS')
        local_fs_file_1.set_attributes(
            directory_template='/tmp',
            files_prefix=f'stf_a_{destination_table_name}',
            files_suffix='data',
            data_format='JSON'
        )
        dev_raw_data_source >> local_fs_file_1
        file_1_pipeline = builder_file_1.build()
        sdc_executor.add_pipeline(file_1_pipeline)
        sdc_executor.start_pipeline(file_1_pipeline).wait_for_finished()

        # Start the pipeline main pipeline
        sdc_executor.start_pipeline(synapse_pipeline)

        # Creates the second file that will trigger the data drift
        builder_file_2 = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder_file_2.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(
            data_format='JSON',
            raw_data='{"NAME": "Jenni Brooks", "ADDRESS": "123 Maple Lane", ''"SHOE-SIZE": 11}',
            stop_after_first_batch=True
        )
        local_fs_file_2 = builder_file_2.add_stage('Local FS')
        local_fs_file_2.set_attributes(
            directory_template='/tmp',
            files_prefix=f'stf_b_{destination_table_name}',
            files_suffix='data',
            data_format='JSON'
        )
        dev_raw_data_source >> local_fs_file_2
        file_2_pipeline = builder_file_2.build()
        sdc_executor.add_pipeline(file_2_pipeline)
        sdc_executor.start_pipeline(file_2_pipeline).wait_for_finished()

        sdc_executor.wait_for_pipeline_metric(synapse_pipeline, 'input_record_count', 2)

        # Assert data is the expected
        result = azure.synapse.engine.execute(f'select * from {azure.synapse_database_schema}.{destination_table_name}')
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()
        assert data_from_database == [
            ("Jenni Brooks", "123 Maple Lane", "11" if create_new_columns_as_varchar else 11),
            ("Mark Brooks", "123 Maple Lane", None)
        ]

        # Assert new column types are the expected
        stmt = (f' select col.name as column_name, t.name as data_type'
                f' from sys.tables as tab'
                f' inner join sys.columns as col on tab.object_id = col.object_id'
                f' left join sys.types as t on col.user_type_id = t.user_type_id'
                f' where tab.name=\'{destination_table_name}\' and (col.name=\'SHOE-SIZE\') order by col.name;')
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        assert data_from_database == [('SHOE-SIZE', 'varchar' if create_new_columns_as_varchar else 'int')]
    finally:
        try:
            delete_table(azure.synapse.engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


def test_synapse_destination_missing_fields(sdc_builder, sdc_executor, azure):
    """
    Test for Azure Synapse target stage. Send an initial batch of records, then send a second
    batch with a missing column. When 'ignore missing fields is active', the batch should be inserted with
    a NULL value for the missing columns

    The pipelines look like:
    dev_raw_data_source >> azure_synapse_destination
    """
    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'

    drift_synapse_pipeline = get_synapse_pipeline(
        sdc_builder,
        azure,
        EXTENDED_ROWS_IN_DATABASE,
        stage_file_prefix,
        destination_table_name
    )
    synapse_pipeline = get_synapse_pipeline(
        sdc_builder,
        azure,
        ROWS_IN_DATABASE,
        stage_file_prefix,
        destination_table_name
    )
    sdc_executor.add_pipeline(drift_synapse_pipeline)
    sdc_executor.add_pipeline(synapse_pipeline)
    engine = azure.synapse.engine

    try:
        # Run pipeline with name & country columns
        sdc_executor.start_pipeline(drift_synapse_pipeline).wait_for_finished()

        # Run pipeline missing the country column
        sdc_executor.start_pipeline(synapse_pipeline).wait_for_finished()

        stmt = f'select * from {azure.synapse_database_schema}.{destination_table_name}'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        assert data_from_database == [(row['id'], row['name'], row['country'], row['birth'])
                                      for row in EXPECTED_DRIFT_RESULT]
    finally:
        try:
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, synapse_pipeline)
            stop_pipeline(sdc_executor, drift_synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


def test_synapse_destination_invalid_fields(sdc_builder, sdc_executor, azure):
    """
    Test for Azure Synapse target stage. Send an initial batch of records, then send a second
    batch with incorrect data types. We send an integer for the Varchar country column -which can be converted-
    and a String value for the Integer year of birth -which can not be converted. When
    'ignore missing fields is active', the batch should be inserted with a NULL value for the invalid year of birth.

    The pipelines look like:
    dev_raw_data_source >> azure_synapse_destination
    """
    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'

    drift_synapse_pipeline = get_synapse_pipeline(
        sdc_builder,
        azure,
        EXTENDED_ROWS_IN_DATABASE,
        stage_file_prefix,
        destination_table_name
    )
    invalid_typesdata_synapse_pipeline = get_synapse_pipeline(
        sdc_builder,
        azure,
        INVALID_TYPES_ROWS_IN_DATABASE,
        stage_file_prefix,
        destination_table_name
    )

    sdc_executor.add_pipeline(drift_synapse_pipeline)
    sdc_executor.add_pipeline(invalid_typesdata_synapse_pipeline)
    engine = azure.synapse.engine
    try:
        # Run regular pipeline
        sdc_executor.start_pipeline(drift_synapse_pipeline).wait_for_finished()

        # Run pipeline with invalid country & birth data types
        sdc_executor.start_pipeline(invalid_typesdata_synapse_pipeline).wait_for_finished()

        stmt = f'select * from {azure.synapse_database_schema}.{destination_table_name}'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        assert data_from_database == [(row['id'], row['name'], row['country'], row['birth'])
                                      for row in EXPECTED_DRIFT_RESULT_WITH_INVALID_TYPES_ROWS]
    finally:
        try:
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, invalid_typesdata_synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


def test_synapse_handle_directory_template_starting_with_slash(sdc_builder, sdc_executor, azure):
    def resolve_staging_dir(fs):
        base_paths = dl_fs.ls(base_directory).response.json()['paths']
        test_directories = [item['name'] for item in base_paths
                            if item['name'].startswith(f'{base_directory}/{directory_prefix}')] if base_paths else []
        return test_directories[0] if len(test_directories) == 1 else None

    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)
    base_directory = f'tmp/out'
    directory_prefix = f'stf_purge_{get_random_string(string.ascii_letters, 10)}'
    directory_template = f'/{base_directory}/{directory_prefix}-${{YYYY()}}-${{MM()}}-${{DD()}}'
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'

    # Build pipeline with parametrized purge staged file option
    synapse_pipeline = get_synapse_pipeline(
        sdc_builder,
        azure,
        ROWS_IN_DATABASE,
        stage_file_prefix,
        destination_table_name,
        purge_stage_file_after_loading=True,
        directory_template=directory_template
    )

    sdc_executor.add_pipeline(synapse_pipeline)
    engine = azure.synapse.engine
    try:
        # Run pipeline
        sdc_executor.start_pipeline(synapse_pipeline).wait_for_finished()

        # Assert run
        stmt = f'select * from {azure.synapse_database_schema}.{destination_table_name}'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()
        assert data_from_database == [(row['id'], row['name']) for row in ROWS_IN_DATABASE]
    finally:
        try:
            # need to retrieve the staging directory again, since it might be the case that an exception is thrown
            # before it is resolved the first time
            dl_fs = azure.datalake.file_system
            staging_directory = resolve_staging_dir(dl_fs)
            if staging_directory is not None:
                clean_up(dl_fs, staging_directory)
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


@pytest.mark.parametrize('purge_stage_file', [True, False])
def test_synapse_purge_staged_files(sdc_builder, sdc_executor, azure, purge_stage_file):
    """
    Test for Azure Synapse target stage. We do so by running a dev raw data source to
    Azure Synapse destination and then reading data from SQL Server database for assertion.

    The pipeline looks like:
    dev_raw_data_source >> azure_synapse_destination
    """

    def resolve_staging_dir(fs):
        base_paths = dl_fs.ls(base_directory).response.json()['paths']
        test_directories = [item['name'] for item in base_paths
                            if item['name'].startswith(f'{base_directory}/{directory_prefix}')] if base_paths else []
        return test_directories[0] if len(test_directories) == 1 else None

    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)
    base_directory = f'tmp/out'
    directory_prefix = f'stf_purge_{get_random_string(string.ascii_letters, 10)}'
    directory_template = f'{base_directory}/{directory_prefix}-${{YYYY()}}-${{MM()}}-${{DD()}}'
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'

    # Build pipeline with parametrized purge staged file option
    synapse_pipeline = get_synapse_pipeline(
        sdc_builder,
        azure,
        ROWS_IN_DATABASE,
        stage_file_prefix,
        destination_table_name,
        purge_stage_file_after_loading=purge_stage_file,
        directory_template=directory_template
    )

    sdc_executor.add_pipeline(synapse_pipeline)
    engine = azure.synapse.engine
    try:
        # Run pipeline
        sdc_executor.start_pipeline(synapse_pipeline).wait_for_finished()

        # Assert run
        stmt = f'select * from {azure.synapse_database_schema}.{destination_table_name}'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()
        assert data_from_database == [(row['id'], row['name']) for row in ROWS_IN_DATABASE]

        # Retrieve stage files, allow some time to rename from _tmp_<filename> to <filename> yet
        dl_fs = azure.datalake.file_system
        staging_directory = resolve_staging_dir(dl_fs)
        if not purge_stage_file:
            assert staging_directory is not None
            assert re.compile(
                f'^{base_directory}/{directory_prefix}-\d{{4}}-\d{{2}}-\d{{2}}$').fullmatch(staging_directory)
        paths = dl_fs.ls(staging_directory).response.json()['paths'] if staging_directory else []
        dl_files = [item['name'] for item in paths] if paths else []
        count = 0
        while count < 10 and (len(dl_files) < 1 or dl_files[0].split('/')[-1].startswith('_tmp_')):
            paths = dl_fs.ls(staging_directory).response.json()['paths']
            dl_files = [item['name'] for item in paths] if paths else []
            sleep(1)
            count += 1

        # Assert files have been properly generated & kept/deleted
        if purge_stage_file:
            assert len(dl_files) == 0
        else:
            assert len(dl_files) > 0
            for file in dl_files:
                dl_file_name = file.split('/')[-1]
                assert dl_file_name.startswith(stage_file_prefix)

    finally:
        try:
            # need to retrieve the staging directory again, since it might be the case that an exception is thrown
            # before it is resolved the first time
            dl_fs = azure.datalake.file_system
            staging_directory = resolve_staging_dir(dl_fs)
            if staging_directory is not None:
                clean_up(dl_fs, staging_directory)
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


def test_partition_table_column_invalid(sdc_builder, sdc_executor, azure):
    """
    Test for Azure Synapse target stage. Partition column does not exist.
    Stage exception is asserted.
    The pipeline looks like:
    dev_raw_data_source >> azure_synapse_destination
    """

    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)

    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'
    raw_data = json.dumps(ROWS_IN_DATABASE)
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=raw_data,
        stop_after_first_batch=True
    )

    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)

    partition_values = ['b', 'c']
    if Version(azure_synapse_destination.stage_version) > Version('1'):
        partition_values = ','.join(partition_values)

    azure_synapse_destination.set_attributes(
        auto_create_table=True,
        table=destination_table_name,
        enable_data_drift=True,
        ignore_missing_fields=True,
        ignore_fields_with_invalid_types=True,
        purge_stage_file_after_loading=True,
        stage_file_prefix=stage_file_prefix,
        partition_table=True,
        partition_column='Xpartition',
        partition_values=partition_values
    )

    wiretap = builder.add_wiretap()
    dev_raw_data_source >> [azure_synapse_destination, wiretap.destination]

    synapse_pipeline = builder.build().configure_for_environment(azure)

    sdc_executor.add_pipeline(synapse_pipeline)

    try:
        sdc_executor.start_pipeline(synapse_pipeline).wait_for_finished()

        logger.info("Verify that destination stage produced 3 error record...")
        # Verify that we have exactly three record

        assert len(wiretap.error_records) == 3
        for i in range(len(wiretap.error_records)):
            assert wiretap.error_records[i].header['errorCode'] == 'AZURE_DATA_WAREHOUSE_10'
            assert 'AZURE_DATA_WAREHOUSE_15' in wiretap.error_records[i].header['errorMessage']
    finally:
        try:
            stop_pipeline(sdc_executor, synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


def test_partition_table_el(sdc_builder, sdc_executor, azure):
    """
    Test for Azure Synapse target stage. We do so by running a dev raw data source to
    Azure Synapse destination and then reading data from SQL Server database for assertion.
    Partition table attribute is tested. Configurations for table partition are passed as Record ELs.
    The pipeline looks like:
    dev_raw_data_source >> azure_synapse_destination
    """

    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)

    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'

    raw_data = json.dumps(ROWS_IN_DATABASE_WITH_PARTITION)

    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=raw_data,
        stop_after_first_batch=True
    )

    expression_evaluator = builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [
        {
            'attributeToSet': 'table',
            'headerAttributeExpression': destination_table_name
        }, {
            'attributeToSet': 'partitionColumn',
            'headerAttributeExpression': 'partition'
        }, {
            'attributeToSet': 'partitionValues',
            'headerAttributeExpression': 'b,c'
        }
    ]

    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        auto_create_table=True,
        table='${record:attribute(\'table\')}',
        enable_data_drift=True,
        ignore_missing_fields=True,
        ignore_fields_with_invalid_types=True,
        purge_stage_file_after_loading=True,
        stage_file_prefix=stage_file_prefix,
        partition_table=True,
        partition_column='${record:attribute(\'partitionColumn\')}',
        partition_values='${record:attribute(\'partitionValues\')}'
    )

    dev_raw_data_source >> expression_evaluator >> azure_synapse_destination

    synapse_pipeline = builder.build().configure_for_environment(azure)

    sdc_executor.add_pipeline(synapse_pipeline)
    engine = azure.synapse.engine
    try:
        sdc_executor.start_pipeline(synapse_pipeline).wait_for_finished()

        stmt = f'select * from [{azure.synapse_database_schema}].[{destination_table_name}]'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        assert data_from_database == [(row['id'], row['name'], row['partition'])
                                      for row in ROWS_IN_DATABASE_WITH_PARTITION]

        stmt = (f'Select s.name As SchemaName, t.name, Count(*) As TableName '
                f'From sys.tables t '
                f'Inner Join sys.schemas s On t.schema_id = s.schema_id '
                f'Inner Join sys.partitions p on p.object_id = t.object_id '
                f'Where p.index_id In (0, 1) '
                f'Group By s.name, t.name Having Count(*) > 1 '
                f' and t.name=\'{destination_table_name}\' Order By s.name, t.name;')

        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        assert len(data_from_database) == 1
        assert data_from_database == [(azure.synapse_database_schema, destination_table_name.upper(), 3)]

    finally:
        try:
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


def test_distribute_table_el(sdc_builder, sdc_executor, azure):
    """
    Test for Azure Synapse target stage. We do so by running a dev raw data source to
    Azure Synapse destination and then reading data from SQL Server database for assertion.
    Distribute table attribute is tested. Configurations for table distribution are passed as Record ELs.
    The pipeline looks like:
    dev_raw_data_source >> azure_synapse_destination
    """

    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)

    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'

    raw_data = json.dumps(ROWS_IN_DATABASE_WITH_PARTITION)

    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=raw_data,
        stop_after_first_batch=True
    )

    expression_evaluator = builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [
        {
            'attributeToSet': 'table',
            'headerAttributeExpression': destination_table_name
        }, {
            'attributeToSet': 'distributionColumns',
            'headerAttributeExpression': 'partition'
        }, {
            'attributeToSet': 'partitionValues',
            'headerAttributeExpression': 'b,c'
        }
    ]

    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        auto_create_table=True,
        table='${record:attribute(\'table\')}',
        enable_data_drift=True,
        ignore_missing_fields=True,
        ignore_fields_with_invalid_types=True,
        purge_stage_file_after_loading=True,
        stage_file_prefix=stage_file_prefix,
        distribute_table=True,
        distribution_columns='${record:attribute(\'distributionColumns\')}'
    )

    dev_raw_data_source >> expression_evaluator >> azure_synapse_destination

    synapse_pipeline = builder.build().configure_for_environment(azure)

    sdc_executor.add_pipeline(synapse_pipeline)
    engine = azure.synapse.engine
    try:
        sdc_executor.start_pipeline(synapse_pipeline).wait_for_finished()

        stmt = f'select * from [{azure.synapse_database_schema}].[{destination_table_name}]'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        assert data_from_database == [(row['id'], row['name'], row['partition'])
                                      for row in ROWS_IN_DATABASE_WITH_PARTITION]

        stmt = (f'Select p.distribution_policy '
                f'From sys.tables t '
                f'Inner Join sys.schemas s On t.schema_id = s.schema_id '
                f'Inner Join sys.pdw_table_distribution_properties p on p.object_id = t.object_id '
                f'Where t.name=\'{destination_table_name}\' ;')

        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        assert data_from_database == [(2,)]

        stmt = (f'Select c.name, p.distribution_ordinal '
                f'From sys.tables t '
                f'Inner Join sys.schemas s On t.schema_id = s.schema_id '
                f'Inner Join sys.columns c on t.object_id = c.object_id '
                f'Inner Join sys.pdw_column_distribution_properties p on p.object_id = t.object_id'
                f' and c.column_id=p.column_id '
                f'Where t.name=\'{destination_table_name}\' ;')

        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()

        assert data_from_database == [('ID', 0), ('NAME', 0), ('PARTITION', 1)]

    finally:
        try:
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


def test_cdc_synapse_multiple_ops_two_batches(sdc_builder, sdc_executor, azure):
    """
    Test for Azure Synapse destination stage. Data is inserted into Azure Synapse using the pipeline.
    After pipeline is run, data is read from Azure Synapse using SQL Server database.
    We insert data in two runs.
    We assert the data from the client to what has been ingested by the Azure Synapse pipeline.
    The data is in CDC format. To do that:
    - A dev raw data source stage generates the data adding an 'op' field and a table name
    - An expression evaluator adds the op field in the header
    - A field remover removes the op field
    - A Azure Synapse stage adds the data

    The pipeline looks like:
    Azure Synapse pipeline:
        dev_raw_data_source  >>  Expression Evaluator >> Field Remover >> azure_synapse_destination
    """
    table_name_1 = f'stf_{get_random_string(string.ascii_uppercase, 10)}'
    stage_name = f'stf_{get_random_string(string.ascii_uppercase, 10)}'
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'

    cdc_rows_mult_ops_1 = [
        {
            'OP': 4,
            'ID': 1,
            'NAME': 'Rogelio Federer'
        }, {
            'OP': 4,
            'ID': 1,
            'NAME': 'Rafa Nadal'
        }
    ]
    cdc_rows_mult_ops_2 = [
        {
            'OP': 4,
            'ID': 1,
            'NAME': 'Domi Thiem'
        }, {
            'OP': 4,
            'ID': 1,
            'NAME': 'Juan Del Potro'
        }, {
            'OP': 2,
            'ID': 1,
            'NAME': 'Juan Del Potro'
        }
    ]

    table_key_columns = [
        {"keyColumns": ["ID"], "table": table_name_1}
    ]

    raw_data = json.dumps(cdc_rows_mult_ops_1)

    engine = azure.synapse.engine

    # Build the pipeline with created entities in Azure Synapse stage configurations.
    builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')

    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=raw_data,
        stop_after_first_batch=True
    )

    # Build Expression Evaluator
    expression_evaluator = builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(
        header_attribute_expressions=[
            {
                'attributeToSet': 'table',
                'headerAttributeExpression': table_name_1
            }, {
                'attributeToSet': 'sdc.operation.type',
                'headerAttributeExpression': "${record:value('/OP')}"
            }
        ]
    )
    # Build Field Remover
    field_remover = builder.add_stage('Field Remover')
    field_remover.fields = ['/OP']

    # Build Azure Synapse
    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        auto_create_table=True,
        table='${record:attribute(\'table\')}',
        key_columns=table_key_columns,
        purge_stage_file_after_loading=True,
        stage_name=stage_name,
        stage_file_prefix=stage_file_prefix,
        merge_cdc_data=False
    )

    dev_raw_data_source >> expression_evaluator >> field_remover >> azure_synapse_destination

    pipeline = (builder.build(title='CDC Azure Synapse destination Multiple Ops')).configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=600)

        # ID 1 Row is in Table 1.
        stmt = f'select * from [{azure.synapse_database_schema}].[{table_name_1}]'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
        result.close()

        # Data from Azure Synapse Database is compared with expected Data.
        # Expected Data is 1 Row with name Rafa Nadal
        inserted_data = [(row['ID'], row['NAME']) for row in cdc_rows_mult_ops_1[1:]]
        assert data_from_database[:1] == inserted_data

        raw_data = '\n'.join((json.dumps(row) for row in cdc_rows_mult_ops_2))
        pipeline[0].configuration.update({'rawData': raw_data})
        sdc_executor.update_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=600)

    finally:
        try:
            delete_table(engine, table_name_1, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, pipeline)
        except Exception as ex:
            logger.error(ex)


@pytest.mark.parametrize('threads', [5, 25])
def test_table_el_eval_azure_synapse(sdc_builder, sdc_executor, azure, threads):
    """
    Test Azure Synapse destination is able to split into different tables based on EL using different threads
    - A directory origin stage generates the data
    - A Azure Synapse stage tries to send the data

    The pipeline looks like:
    Azure Synapse pipeline:
        directory >> azure_synapse_destination
        directory >= file_finished_finisher
    """
    table_name = f'stf_table_{get_random_string(string.ascii_lowercase, 5)}'
    stage_name = f'stf_{get_random_string(string.ascii_uppercase, 5)}'
    engine = azure.synapse.engine
    data = []

    for i in range(0, 1000):
        data.append({'TABLE': f'{table_name}_{random.randint(1, threads)}', 'ID': i, 'NAME': random.randint(6, 9)})

    raw_data = '\n'.join((json.dumps(row) for row in data))

    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string())

    sdc_executor.execute_shell(f'mkdir -p {tmp_directory}')
    sdc_executor.write_file(os.path.join(tmp_directory, 'sdc1.txt'), raw_data)

    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(
        data_format='JSON',
        file_name_pattern='sdc*.txt',
        file_name_pattern_mode='GLOB',
        files_directory=tmp_directory,
        process_subdirectories=True,
        read_order='TIMESTAMP',
        batch_size_in_recs=50,
        number_of_threads=threads
    )

    file_finished_finisher = builder.add_stage('Pipeline Finisher Executor')
    file_finished_finisher.set_attributes(
        stage_record_preconditions=["${record:eventType() == 'finished-file'}"]
    )

    # Build Azure Synapse stage.
    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        connection_pool_size=threads,
        table="${record:value('/TABLE')}",
        stage_name=stage_name,
        purge_stage_file_after_loading=True,
        row_field='/'
    )

    directory >> azure_synapse_destination
    directory >= file_finished_finisher

    pipeline = (builder.build(title='Directory to Azure Synapse using Els')).configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)

    try:
        tables = []
        for i in range(1, threads):
            table = sqlalchemy.Table(f'{table_name}_{i}',
                                     sqlalchemy.MetaData(),
                                     sqlalchemy.Column('ID', sqlalchemy.Integer),
                                     sqlalchemy.Column('NAME', sqlalchemy.Integer))
            tables.append(table)
            table.create(engine)

        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=1800)

        for i in range(1, threads):
            stmt = f'select * from [{azure.synapse_database_schema}].[{table_name}_{i}]'
            result = engine.execute(stmt)
            data_from_database = sorted(result.fetchall(), key=lambda row: row[1])
            result.close()
            inserted_data = [(row['ID'], row['NAME']) for row in list(filter(lambda x: x['TABLE'] == f'{table_name}_{i}', data))]
            if len(inserted_data) != len(data_from_database):
                print("inserted_data")
                print(inserted_data)
                print("data_from_database")
                print(data_from_database)
            assert len(inserted_data) == len(data_from_database)
            for j in range(len(inserted_data)):
                assert inserted_data[j] == data_from_database[j][1:]

    finally:
        try:
            for i in range(1, threads):
                delete_table(engine, f'{table_name}_{i}', azure.synapse_database_schema)
            stop_pipeline(sdc_executor, pipeline)
        except Exception as ex:
            logger.error(ex)


@pytest.mark.parametrize('auto_create', ["NONE", "TABLE", "SCHEMA"])
def test_table_and_schema_el_eval_azure_synapse(sdc_builder, sdc_executor, azure, auto_create):
    """
    Test Azure Synapse destination is able to split into different schema+tables based on EL using different threads
    - A directory origin stage generates the data
    - A Azure Synapse stage tries to send the data

    When auto creating schemas, we also need to auto create tables.
    If there is no schema we cannot create it beforehand

    The pipeline looks like:
    Azure Synapse pipeline:
        directory >> azure_synapse_destination
        directory >= file_finished_finisher
    """
    auto_create_schema = auto_create in ["SCHEMA"]
    auto_create_table = auto_create in ["TABLE", "SCHEMA"]

    schema_name = f'STF_SCHEMA_{get_random_string(string.ascii_uppercase, 5)}'
    schema_name_1 = f'{schema_name}_1'
    schema_name_2 = f'{schema_name}_2'
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    table_name_1 = f'{table_name}_1'
    table_name_2 = f'{table_name}_2'
    stage_name = f'STF_{get_random_string(string.ascii_uppercase, 5)}'
    data = []

    for i in range(0, 500):
        record = {
            'TABLE': f'{table_name}_{random.randint(1, 2)}',
            'SCHEMA': f'{schema_name}_{random.randint(1, 2)}',
            'ID': i, 'NAME': random.randint(6, 9)
        }
        data.append(record)

    raw_data = '\n'.join((json.dumps(row) for row in data))

    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string())

    sdc_executor.execute_shell(f'mkdir -p {tmp_directory}')
    sdc_executor.write_file(os.path.join(tmp_directory, 'sdc1.txt'), raw_data)

    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(
        data_format='JSON',
        file_name_pattern='sdc*.txt',
        file_name_pattern_mode='GLOB',
        files_directory=tmp_directory,
        process_subdirectories=True,
        read_order='TIMESTAMP',
        batch_size_in_recs=50,
        number_of_threads=5
    )

    file_finished_finisher = builder.add_stage('Pipeline Finisher Executor')
    file_finished_finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'finished-file'}"])

    # Build Azure Synapse stage.
    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        connection_pool_size=5,
        auto_create_table=auto_create_table,
        auto_create_schema=auto_create_schema,
        table="${record:value('/TABLE')}",
        stage_name=stage_name,
        purge_stage_file_after_loading=True,
        row_field='/'
    )

    directory >> azure_synapse_destination
    directory >= file_finished_finisher

    pipeline = (builder.build(title='Directory to Azure Synapse using Els')).configure_for_environment(azure)
    azure_synapse_destination.set_attributes(
        schema="${record:value('/SCHEMA')}"
    )

    sdc_executor.add_pipeline(pipeline)

    engine = azure.synapse.engine
    try:
        if not auto_create_schema:
            # Create schema
            logger.info(f'Creating Schemas {schema_name_1} and {schema_name_2}')
            engine.execute(f"CREATE SCHEMA {schema_name_1}")
            engine.execute(f"CREATE SCHEMA {schema_name_2}")

        if not auto_create_table:
            # Create tables inside schemas (2 tables inside 2 schemas = 4 tables)
            logger.info(f'Creating Table {schema_name_1}.{table_name_1}')
            engine.execute(f"""
                CREATE TABLE {schema_name_1}.{table_name_1}
                ("ID" INTEGER NULL, "NAME" INTEGER NULL)
            """)

            logger.info(f'Creating Table {schema_name_1}.{table_name_2}')
            engine.execute(f"""
                CREATE TABLE {schema_name_1}.{table_name_2}
                ("ID" INTEGER NULL, "NAME" INTEGER NULL)
            """)

            logger.info(f'Creating Table {schema_name_2}.{table_name_1}')
            engine.execute(f"""
                CREATE TABLE {schema_name_2}.{table_name_1}
                ("ID" INTEGER NULL, "NAME" INTEGER NULL)
            """)

            logger.info(f'Creating Table {schema_name_2}.{table_name_2}')
            engine.execute(f"""
                CREATE TABLE {schema_name_2}.{table_name_2}
                ("ID" INTEGER NULL, "NAME" INTEGER NULL)
            """)

        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=900)

        stmt = f'select ID, NAME from [{schema_name_1}].[{table_name_1}]'
        result = engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()
        inserted_data = [(row['ID'], row['NAME']) for row in list(
            filter(lambda x: x['SCHEMA'] == schema_name_1 and x['TABLE'] == table_name_1, data))]
        if len(inserted_data) != len(data_from_database):
            print("inserted_data")
            print(inserted_data)
            print("data_from_database")
            print(data_from_database)
        assert len(inserted_data) == len(data_from_database)
        for i in range(len(inserted_data)):
            assert inserted_data[i] == data_from_database[i]

        stmt = f'select ID, NAME from [{schema_name_1}].[{table_name_2}]'
        result = engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()
        inserted_data = [(row['ID'], row['NAME']) for row in list(
            filter(lambda x: x['SCHEMA'] == schema_name_1 and x['TABLE'] == table_name_2, data))]
        if len(inserted_data) != len(data_from_database):
            print("inserted_data")
            print(inserted_data)
            print("data_from_database")
            print(data_from_database)
        assert len(inserted_data) == len(data_from_database)
        for i in range(len(inserted_data)):
            assert inserted_data[i] == data_from_database[i]

        stmt = f'select ID, NAME from [{schema_name_2}].[{table_name_1}]'
        result = engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        inserted_data = [(row['ID'], row['NAME']) for row in list(
            filter(lambda x: x['SCHEMA'] == schema_name_2 and x['TABLE'] == table_name_1, data))]
        assert len(inserted_data) == len(data_from_database)
        if len(inserted_data) != len(data_from_database):
            print("inserted_data")
            print(inserted_data)
            print("data_from_database")
            print(data_from_database)
        for i in range(len(inserted_data)):
            assert inserted_data[i] == data_from_database[i]

        stmt = f'select ID, NAME from [{schema_name_2}].[{table_name_2}]'
        result = engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        inserted_data = [(row['ID'], row['NAME']) for row in list(
            filter(lambda x: x['SCHEMA'] == schema_name_2 and x['TABLE'] == table_name_2, data))]
        if len(inserted_data) != len(data_from_database):
            print("inserted_data")
            print(inserted_data)
            print("data_from_database")
            print(data_from_database)
        assert len(inserted_data) == len(data_from_database)
        for i in range(len(inserted_data)):
            assert inserted_data[i] == data_from_database[i]
    finally:
        try:
            stop_pipeline(sdc_executor, pipeline)
            delete_table(engine, table_name_1, schema_name_1)
            delete_table(engine, table_name_1, schema_name_2)
            delete_table(engine, table_name_2, schema_name_1)
            delete_table(engine, table_name_2, schema_name_2)
            delete_schema(engine, schema_name_1)
            delete_schema(engine, schema_name_2)
        except Exception as ex:
            logger.error(ex)


def test_synapse_merge_cdc_op_4(sdc_builder, sdc_executor, azure):
    table_name = f'stf_{get_random_string(string.ascii_uppercase, 10)}'
    stage_name = f'stf_{get_random_string(string.ascii_uppercase, 10)}'
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'
    cdc_rows = [
        {'ID': 1, 'NAME': 'Rogelio Federer'},
        {'ID': 2, 'NAME': 'Rafa Nadal'}
    ]

    builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=json.dumps(cdc_rows),
        stop_after_first_batch=True
    )

    # Build Expression Evaluator
    expression_evaluator = builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(
        header_attribute_expressions=[{
            'attributeToSet': 'sdc.operation.type',
            'headerAttributeExpression': "4"
        }]
    )

    # Build Azure Synapse
    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        auto_create_table=False,
        table=table_name,
        key_columns=[{
            "keyColumns": ["ID"],
            "table": table_name
        }],
        purge_stage_file_after_loading=True,
        stage_name=stage_name,
        stage_file_prefix=stage_file_prefix,
        merge_cdc_data=True,
        enable_data_drift=False
    )
    # primary key location attribute added in stage version 5
    if Version(azure_synapse_destination.stage_version) > Version('4'):
        azure_synapse_destination.set_attributes(
            primary_key_location="TABLE"
        )

    dev_raw_data_source >> expression_evaluator >> azure_synapse_destination

    pipeline = builder.build(title='CDC Azure Synapse destination Merge CDC').configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)
    try:
        synapse_engine = azure.synapse.engine

        # Create the table with some data with same ID on it. This will force the merge to update the value of already
        # existing rows. It won't update the values for the columns that were used for distribution
        synapse_engine.execute(f"""
            CREATE TABLE [{azure.synapse_database_schema}].[{table_name}]
            ([ID] [int] NOT NULL, [NAME] [char](100) NOT NULL)
            WITH
            (DISTRIBUTION = HASH ([ID]), CLUSTERED COLUMNSTORE INDEX)
        """)
        synapse_engine.execute(f"INSERT INTO [{azure.synapse_database_schema}].[{table_name}] "
                               f"(ID, NAME) VALUES (1, 'Daniil Medvedev')")
        synapse_engine.execute(f"INSERT INTO [{azure.synapse_database_schema}].[{table_name}] "
                               f"(ID, NAME) VALUES (3, 'Dominic Thiem')")

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=600)

        result = synapse_engine.execute(f'select * from [{azure.synapse_database_schema}].[{table_name}]')
        data_from_database = sorted(
            [(r[0], r[1].strip()) for r in result.fetchall()],
            key=lambda row: row[0]
        )

        assert data_from_database[0] == (1, 'Rogelio Federer')
        assert data_from_database[1] == (2, 'Rafa Nadal')
        assert data_from_database[2] == (3, 'Dominic Thiem')
        result.close()
    finally:
        try:
            delete_table(azure.synapse.engine, table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, pipeline)
        except Exception as ex:
            logger.error(ex)


def test_synapse_merge_cdc_op_4_el_table(sdc_builder, sdc_executor, azure):
    table_name_1 = f'stf_{get_random_string(string.ascii_uppercase, 10)}'
    table_name_2 = f'stf_{get_random_string(string.ascii_uppercase, 10)}'
    stage_name = f'stf_{get_random_string(string.ascii_uppercase, 10)}'
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'
    cdc_rows = [
        {'ID': 1, 'NAME': 'Rogelio Federer', 'table': table_name_1},
        {'ID': 1, 'NAME': 'Rafa Nadal', 'table': table_name_2}
    ]

    builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=json.dumps(cdc_rows),
        stop_after_first_batch=True
    )

    # Build Expression Evaluator
    expression_evaluator = builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(
        header_attribute_expressions=[
            {
                'attributeToSet': 'sdc.operation.type',
                'headerAttributeExpression': "4"
            }, {
                'attributeToSet': 'table',
                'headerAttributeExpression': '${record:value("/table")}'
            }
        ]
    )

    # Build Field Remover
    field_remover = builder.add_stage('Field Remover')
    field_remover.fields = ['/table']

    # Build Azure Synapse
    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        auto_create_table=False,
        table='${record:attribute(\'table\')}',
        key_columns=[
            {'keyColumns': ['ID'], 'table': table_name_1},
            {'keyColumns': ['ID'], 'table': table_name_2}
        ],
        purge_stage_file_after_loading=True,
        stage_name=stage_name,
        stage_file_prefix=stage_file_prefix,
        merge_cdc_data=True,
        enable_data_drift=False
    )
    # primary key location attribute added in stage version 5
    if Version(azure_synapse_destination.stage_version) > Version('4'):
        azure_synapse_destination.set_attributes(primary_key_location="TABLE")

    dev_raw_data_source >> expression_evaluator >> field_remover >> azure_synapse_destination

    pipeline = (builder.build(title='CDC Azure Synapse destination Merge CDC Multiple tables')
                .configure_for_environment(azure))
    sdc_executor.add_pipeline(pipeline)
    try:
        synapse_engine = azure.synapse.engine

        for table_name in [table_name_1, table_name_2]:
            synapse_engine.execute(f"""
            CREATE TABLE [{azure.synapse_database_schema}].[{table_name}]
            ([ID] [int] NOT NULL, [NAME] [char](100) NOT NULL)
            WITH (DISTRIBUTION = HASH ([ID]), CLUSTERED COLUMNSTORE INDEX)
            """)
            synapse_engine.execute(f"INSERT INTO [{azure.synapse_database_schema}].[{table_name}] "
                                   f"(ID, NAME) VALUES (1, 'Daniil Medvedev')")

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=600)

        result_1 = synapse_engine.execute(f'select * from [{azure.synapse_database_schema}].[{table_name_1}]')
        data_from_database_1 = sorted(
            [(r[0], r[1].strip()) for r in result_1.fetchall()],
            key=lambda row: row[0]
        )
        result_1.close()

        result_2 = synapse_engine.execute(f'select * from [{azure.synapse_database_schema}].[{table_name_2}]')
        data_from_database_2 = sorted(
            [(r[0], r[1].strip()) for r in result_2.fetchall()],
            key=lambda row: row[0]
        )
        result_2.close()

        assert len(data_from_database_1) == 1
        assert data_from_database_1[0] == (1, 'Rogelio Federer')
        assert len(data_from_database_2) == 1
        assert data_from_database_2[0] == (1, 'Rafa Nadal')
    finally:
        try:
            delete_table(azure.synapse.engine, table_name_1, azure.synapse_database_schema)
            delete_table(azure.synapse.engine, table_name_2, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, pipeline)
        except Exception as ex:
            logger.error(ex)


@pytest.mark.parametrize('staging_authentication', ['OAUTH', 'SHARED_KEY'])
@pytest.mark.parametrize(
    'copy_statement_authentication',
    [None, 'AAD_USER', 'MANAGED_IDENTITY', 'SAS', 'SERVICE_PRINCIPALS', 'STORAGE_ACCOUNT_KEY']
)
def test_synapse_destination_authentications_azure_active_directory(
         sdc_builder,
         sdc_executor,
         azure,
         staging_authentication,
         copy_statement_authentication
):
    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'

    raw_data = json.dumps(ROWS_IN_DATABASE)
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=raw_data,
        stop_after_first_batch=True
    )

    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        on_record_error="STOP_PIPELINE",
        auto_create_table=True,
        table=destination_table_name,
        enable_data_drift=True,
        ignore_missing_fields=True,
        ignore_fields_with_invalid_types=True,
        purge_stage_file_after_loading=True,
        stage_file_prefix=stage_file_prefix,
        directory_template='${YYYY()}-${MM()}-${DD()}-${hh()}',
        staging_authentication=staging_authentication
    )

    azure_synapse_destination.database = azure.synapse_database
    azure_synapse_destination.azure_active_directory_id = azure.synapse_active_directory_id
    azure_synapse_destination.azure_active_directory_password = azure.synapse_active_directory_password

    if staging_authentication == 'OAUTH':
        azure_synapse_destination.application_id = azure.client_id
        azure_synapse_destination.application_key = azure.client_secret
        azure_synapse_destination.tenant_id = azure.tenant_id
    else:
        azure_synapse_destination.storage_account_key = azure.storage_account_key

    if copy_statement_authentication is None:
        azure_synapse_destination.set_attributes(
            specify_copy_statement_authentication=False
        )
    else:
        azure_synapse_destination.set_attributes(
            specify_copy_statement_authentication=True,
            copy_statement_authentication=copy_statement_authentication
        )
        if copy_statement_authentication == 'SAS':
            azure_synapse_destination.sas_token = azure.azure_sas_token
        elif copy_statement_authentication == 'SERVICE_PRINCIPALS':
            azure_synapse_destination.application_id = azure.client_id
            azure_synapse_destination.application_key = azure.client_secret
            azure_synapse_destination.tenant_id = azure.tenant_id
        elif copy_statement_authentication == 'STORAGE_ACCOUNT_KEY':
            azure_synapse_destination.storage_account_key = azure.account_shared_key

    dev_raw_data_source >> azure_synapse_destination

    synapse_pipeline = builder.build().configure_for_environment(azure)

    sdc_executor.add_pipeline(synapse_pipeline)
    synapse_pipeline.stages.get(label=azure_synapse_destination.label).set_attributes(
        azure_synapse_authentication='AZURE_ACTIVE_DIRECTORY'
    )
    sdc_executor.update_pipeline(synapse_pipeline)

    engine = azure.synapse.engine
    try:
        # Run pipeline with name & country columns
        sdc_executor.start_pipeline(synapse_pipeline).wait_for_finished()

        stmt = f'select * from {azure.synapse_database_schema}.{destination_table_name}'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()
        assert data_from_database == [(row['id'], row['name']) for row in ROWS_IN_DATABASE]
    finally:
        try:
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


@pytest.mark.parametrize('staging_authentication', ['OAUTH', 'SHARED_KEY'])
@pytest.mark.parametrize(
    'copy_statement_authentication',
    [None, 'MANAGED_IDENTITY', 'SAS', 'SERVICE_PRINCIPALS', 'STORAGE_ACCOUNT_KEY']
)
def test_synapse_destination_authentications_sql_server_login(
        sdc_builder,
        sdc_executor,
        azure,
        staging_authentication,
        copy_statement_authentication
):
    """
    Test all the staging authentications and copy statement authentication in Azure Synapse.

    The pipelines look like:
    dev_data_source >> azure_synapse_destination
    """
    destination_table_name = f'stf_{get_random_string(string.ascii_letters, 10)}'
    logger.info('destination_table_name = %s', destination_table_name)
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'

    raw_data = json.dumps(ROWS_IN_DATABASE)
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=raw_data,
        stop_after_first_batch=True
    )

    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        auto_create_table=True,
        table=destination_table_name,
        enable_data_drift=True,
        ignore_missing_fields=True,
        ignore_fields_with_invalid_types=True,
        purge_stage_file_after_loading=True,
        stage_file_prefix=stage_file_prefix,
        directory_template='${YYYY()}-${MM()}-${DD()}-${hh()}',
        staging_authentication=staging_authentication
    )

    azure_synapse_destination.user = azure.synapse_user
    azure_synapse_destination.password = azure.synapse_password
    azure_synapse_destination.database = azure.synapse_database

    if staging_authentication == 'OAUTH':
        azure_synapse_destination.application_id = azure.client_id
        azure_synapse_destination.application_key = azure.client_secret
        azure_synapse_destination.tenant_id = azure.tenant_id
    else:
        azure_synapse_destination.storage_account_key = azure.storage_account_key

    if copy_statement_authentication is None:
        azure_synapse_destination.set_attributes(
            specify_copy_statement_authentication=False
        )
    else:
        azure_synapse_destination.set_attributes(
            specify_copy_statement_authentication=True,
            copy_statement_authentication=copy_statement_authentication
        )
        if copy_statement_authentication == 'SAS':
            azure_synapse_destination.sas_token = azure.azure_sas_token
        elif copy_statement_authentication == 'SERVICE_PRINCIPALS':
            azure_synapse_destination.application_id = azure.client_id
            azure_synapse_destination.application_key = azure.client_secret
            azure_synapse_destination.tenant_id = azure.tenant_id
        elif copy_statement_authentication == 'STORAGE_ACCOUNT_KEY':
            azure_synapse_destination.storage_account_key = azure.account_shared_key

    dev_raw_data_source >> azure_synapse_destination

    synapse_pipeline = builder.build().configure_for_environment(azure)

    sdc_executor.add_pipeline(synapse_pipeline)
    synapse_pipeline.stages.get(label=azure_synapse_destination.label).set_attributes(
        azure_synapse_authentication='SQL_SERVER_LOGIN'
    )
    sdc_executor.update_pipeline(synapse_pipeline)

    engine = azure.synapse.engine
    try:
        # Run pipeline with name & country columns
        sdc_executor.start_pipeline(synapse_pipeline).wait_for_finished()

        stmt = f'select * from {azure.synapse_database_schema}.{destination_table_name}'
        result = azure.synapse.engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
        result.close()
        assert data_from_database == [(row['id'], row['name']) for row in ROWS_IN_DATABASE]
    finally:
        try:
            delete_table(engine, destination_table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, synapse_pipeline)
        except Exception as ex:
            logger.error(ex)


def test_synapse_destination_merge_cdc_enable_data_drift(sdc_builder, sdc_executor, azure):
    table_name = f'stf_{get_random_string(string.ascii_uppercase, 10)}'
    stage_name = f'stf_{get_random_string(string.ascii_uppercase, 10)}'
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'
    cdc_rows = [
        {'ID': 1, 'NAME': 'Rogelio Federer', 'CODE': 1},
        {'ID': 2, 'NAME': 'Rafa Nadal', 'CODE': 2}
    ]

    builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=json.dumps(cdc_rows),
        stop_after_first_batch=True
    )

    # Build Expression Evaluator
    expression_evaluator = builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(
        header_attribute_expressions=[{
            'attributeToSet': 'sdc.operation.type',
            'headerAttributeExpression': "4"
        }]
    )

    # Build Azure Synapse
    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        auto_create_table=False,
        table=table_name,
        key_columns=[{"keyColumns": ["ID"], "table": table_name}],
        purge_stage_file_after_loading=True,
        stage_name=stage_name,
        stage_file_prefix=stage_file_prefix,
        merge_cdc_data=True,
        enable_data_drift=True
    )
    # primary key location attribute added in stage version 5
    if Version(azure_synapse_destination.stage_version) > Version('4'):
        azure_synapse_destination.set_attributes(
            primary_key_location="TABLE"
        )

    dev_raw_data_source >> expression_evaluator >> azure_synapse_destination

    pipeline = (builder.build(title='Test CDC Azure Synapse Destination Merge CDC Enable Data Drift')
                .configure_for_environment(azure))
    sdc_executor.add_pipeline(pipeline)
    try:
        synapse_engine = azure.synapse.engine

        # Create the table with some data with same ID on it. This will force the merge to update the value of
        # already existing rows. It won't update the values for the columns that were used for distribution
        synapse_engine.execute(f"""
        CREATE TABLE [{azure.synapse_database_schema}].[{table_name}]
        ([ID] [int] NOT NULL, [NAME] [char](100) NOT NULL)
        WITH (DISTRIBUTION = HASH ([ID]), CLUSTERED COLUMNSTORE INDEX)
        """)

        synapse_engine.execute(f"INSERT INTO [{azure.synapse_database_schema}].[{table_name}] "
                               f"(ID, NAME) VALUES (1, 'Daniil Medvedev')")
        synapse_engine.execute(f"INSERT INTO [{azure.synapse_database_schema}].[{table_name}] "
                               f"(ID, NAME) VALUES (3, 'Dominic Thiem')")

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=600)

        result = synapse_engine.execute(f'select * from [{azure.synapse_database_schema}].[{table_name}]')
        data_from_database = sorted(
            [(r[0], r[1].strip(), r[2]) for r in result.fetchall()],
            key=lambda row: row[0]
        )

        assert data_from_database[0] == (1, 'Rogelio Federer', 1)
        assert data_from_database[1] == (2, 'Rafa Nadal', 2)
        assert data_from_database[2] == (3, 'Dominic Thiem', None)
        result.close()
    finally:
        try:
            delete_table(azure.synapse.engine, table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, pipeline)
        except Exception as ex:
            logger.error(ex)


def test_synapse_destination_jdbc_header(sdc_builder, sdc_executor, azure):
    primary_key_columns = ['TYPE', 'ID']
    table_name = f'stf_{get_random_string(string.ascii_uppercase, 10)}'
    stage_name = f'stf_{get_random_string(string.ascii_uppercase, 10)}'
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'

    builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    rows = CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY
    for row, header in zip(rows, CDC_PK_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY_HEADER):
        row['HEADER'] = header
    # raw_data = (',').('\n').join((json.dumps(row) for row in rows))
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       json_content='ARRAY_OBJECTS',
                                       raw_data=json.dumps(rows),
                                       stop_after_first_batch=True)

    # Build Expression Evaluator
    expression_evaluator = builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {
            'attributeToSet': 'sdc.operation.type',
            'headerAttributeExpression': "${record:value('/HEADER/sdc.operation.type')}"
        }, {
            'attributeToSet': f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID',
            'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_OLD_VALUE + ".ID')}"
        }, {
            'attributeToSet': f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID',
            'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_NEW_VALUE + ".ID')}"
        }, {
            'attributeToSet': f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE',
            'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_OLD_VALUE + ".TYPE')}"
        }, {
            'attributeToSet': f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE',
            'headerAttributeExpression': "${record:value('/HEADER/" + PRIMARY_KEY_COLUMN_NEW_VALUE + ".TYPE')}"
        }, {
            'attributeToSet': f'{PRIMARY_KEY_SPECIFICATION}',
            'headerAttributeExpression': '{\"ID\":{}, \"TYPE\":{}}'
        }
    ])

    # Build Field Remover
    field_remover = builder.add_stage('Field Remover')
    field_remover.fields = ['/HEADER']

    # Build Azure Synapse
    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        auto_create_table=True,
        table=table_name,
        merge_cdc_data=True,
        primary_key_location="HEADER",
        purge_stage_file_after_loading=True,
        stage_name=stage_name,
        stage_file_prefix=stage_file_prefix,
        ignore_missing_fields=True,
        ignore_fields_with_invalid_types=True,
        enable_data_drift=True
    )

    dev_raw_data_source >> expression_evaluator >> field_remover >> azure_synapse_destination

    pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)
    try:
        engine = azure.synapse.engine
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=600)
        result = engine.execute(f'select * from [{azure.synapse_database_schema}].[{table_name}]')
        data_from_database = result.fetchall()
        result.close()

        # we are chain updating, it has to equal last record sent
        expected_data = rows[6]
        assert data_from_database == [(expected_data['TYPE'], expected_data['ID'],
                                       expected_data['NAME'], expected_data['SURNAME'], expected_data['ADDRESS'])]
    finally:
        try:
            delete_table(azure.synapse.engine, table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, pipeline)
        except Exception as ex:
            logger.error(ex)


def test_synapse_destination_table_with_pk_and_enable_data_drift(sdc_builder, sdc_executor, azure):
    table_name = f'stf_{get_random_string(string.ascii_uppercase, 10)}'
    stage_name = f'stf_{get_random_string(string.ascii_uppercase, 10)}'
    stage_file_prefix = f'stf_{get_random_string(string.ascii_letters, 10)}'
    cdc_rows = [
        {'ID': 1, 'NAME': 'Rogelio Federer', 'CODE': 1},
        {'ID': 2, 'NAME': 'Rafa Nadal', 'CODE': 2}
    ]

    builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=json.dumps(cdc_rows),
        stop_after_first_batch=True
    )

    # Build Expression Evaluator
    expression_evaluator = builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(
        header_attribute_expressions=[{
            'attributeToSet': 'sdc.operation.type',
            'headerAttributeExpression': "4"
        }]
    )

    # Build Azure Synapse
    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        auto_create_table=False,
        table=table_name,
        key_columns=[{"keyColumns": ["ID"], "table": table_name}],
        primary_key_location="TABLE",
        purge_stage_file_after_loading=True,
        stage_name=stage_name,
        stage_file_prefix=stage_file_prefix,
        merge_cdc_data=True,
        enable_data_drift=True
    )

    dev_raw_data_source >> expression_evaluator >> azure_synapse_destination

    pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)
    try:
        synapse_engine = azure.synapse.engine

        # Create the table with some data with same ID on it. This will force the merge to update the value of
        # already existing rows. It won't update the values for the columns that were used for distribution
        synapse_engine.execute(f"""
        CREATE TABLE [{azure.synapse_database_schema}].[{table_name}]
        ([ID] [int] PRIMARY KEY NONCLUSTERED NOT ENFORCED, [NAME] [char](100) NOT NULL)
        WITH (DISTRIBUTION = HASH ([ID]))
        """)

        synapse_engine.execute(f"INSERT INTO [{azure.synapse_database_schema}].[{table_name}] "
                               f"(ID, NAME) VALUES (1, 'Daniil Medvedev')")
        synapse_engine.execute(f"INSERT INTO [{azure.synapse_database_schema}].[{table_name}] "
                               f"(ID, NAME) VALUES (3, 'Dominic Thiem')")

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=600)

        result = synapse_engine.execute(f'select * from [{azure.synapse_database_schema}].[{table_name}]')
        data_from_database = sorted(
            [(r[0], r[1].strip(), r[2]) for r in result.fetchall()],
            key=lambda row: row[0]
        )

        assert data_from_database[0] == (1, 'Rogelio Federer', 1)
        assert data_from_database[1] == (2, 'Rafa Nadal', 2)
        assert data_from_database[2] == (3, 'Dominic Thiem', None)
        result.close()
    finally:
        try:
            delete_table(azure.synapse.engine, table_name, azure.synapse_database_schema)
            stop_pipeline(sdc_executor, pipeline)
        except Exception as ex:
            logger.error(ex)
