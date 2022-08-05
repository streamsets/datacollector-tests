# Copyright 2017 StreamSets Inc.
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
import sqlalchemy
import string

from stage.utils.utils_primary_key_metadata import PRIMARY_KEY_NON_NUMERIC_METADATA_SQLSERVER, \
    PRIMARY_KEY_NUMERIC_METADATA_SQLSERVER, get_create_table_query_non_numeric, get_create_table_query_numeric, \
    get_insert_query_non_numeric, get_insert_query_numeric, _primary_key_specification_json
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string
from time import sleep

logger = logging.getLogger(__name__)


@database('sqlserver')
@pytest.mark.parametrize('include_record', [True, False])
def test_sql_server_change_tracking_with_composite_primary_key(sdc_builder, sdc_executor, database, include_record):
    """Validate the pipeline on composite primary key. Start the pipeline run the first batch, stop the pipeline and
    restart the pipeline with committed offset. The pipeline will look like:
        sqlserver_change_tracking >> wiretap
    """
    if not database.is_ct_enabled:
        pytest.skip('Test only runs against SQL Server with CT enabled.')

    table_name = get_random_string(string.ascii_lowercase, 20)

    table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                             sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                             sqlalchemy.Column('name', sqlalchemy.String(25), primary_key=True),
                             sqlalchemy.Column('pk', sqlalchemy.Integer, primary_key=True),
                             sqlalchemy.Column('dt', sqlalchemy.String(20)))
    rows_in_database = [{'id': 1, 'name': 'Ji Sun', 'pk': 1, 'dt': '2017-05-03'},
                        {'id': 2, 'name': 'Jarcec', 'pk': 2, 'dt': '2017-05-03'},
                        {'id': 3, 'name': 'Santhosh', 'pk': 3, 'dt': '2017-05-03'}]
    table_configs = [{'initialOffset': 0, 'schema': 'dbo', 'tablePattern': f'{table_name}'}]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    sql_server_change_tracking = pipeline_builder.add_stage('SQL Server Change Tracking Client')
    sql_server_change_tracking.set_attributes(include_the_latest_data_in_the_record=include_record,
                                              table_configs=table_configs)
    wiretap = pipeline_builder.add_wiretap()

    sql_server_change_tracking >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s...', table_name)
        table.create(database.engine)
        connection = database.engine.connect()
        # enable change tracking on table
        connection.execute(f'ALTER TABLE {table_name} ENABLE change_tracking WITH (track_columns_updated = on)')
        # insert sample data
        logger.info('Adding %s rows into %s...', len(rows_in_database), table_name)
        connection.execute(table.insert(), rows_in_database)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(len(rows_in_database))
        sdc_executor.stop_pipeline(pipeline)

        # Let's validate offset
        offset = sdc_executor.api_client.get_pipeline_committed_offsets(pipeline.id).response.json()
        assert offset is not None
        assert offset['offsets'] is not None
        expected_key = [f"tableName=dbo.{table_name};;;partitioned=false;;;partitionSequence=-1;;;partitionStartOffsets=;;;partitionMaxOffsets=;;;usingNonIncrementalLoad=false"]
        assert list(offset['offsets'].keys()) == expected_key

        # delete sample data
        logger.info('Deleting %s rows into %s...', len(rows_in_database), table_name)
        connection.execute(table.delete(), rows_in_database)
        sdc_executor.start_pipeline(pipeline)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3)
        sdc_executor.stop_pipeline(pipeline)

        # assert all the data captured have the same raw_data
        output_records = wiretap.output_records

        assert 6 == len(output_records)
        for i in range(0, 3):
            assert output_records[i].get_field_data('/id') == rows_in_database[i].get('id')
        # Wiretap duplicates some records
        for i in range(3, 6):
            assert output_records[i].get_field_data('/id') == rows_in_database[i - 3].get('id')

        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 3
    finally:
        logger.info('Dropping table %s...', table_name)
        table.drop(database.engine)


@sdc_min_version('5.2.0')
@database('sqlserver')
def test_primary_keys_headers(sdc_builder, sdc_executor, database):
    """
    Test to check the values of the primary keys are present in the headers of the update output records.

    SQL Server does not consider an update as such if the primary keys are changed (in such case it is considered a
    delete + an insert), so the before and after values will always match.
    """
    if not database.is_ct_enabled:
        pytest.skip('Test only runs against SQL Server with CT enabled.')

    pipeline = None
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    try:
        logger.info('Creating source table %s in %s database ...', table_name, database.type)
        table = sqlalchemy.Table(
            table_name,
            sqlalchemy.MetaData(database.engine),
            sqlalchemy.Column('name', sqlalchemy.String(64), primary_key=True),
            sqlalchemy.Column('pokedex_id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('type', sqlalchemy.String(64)),
            sqlalchemy.Column('generation', sqlalchemy.Integer)
        )
        table.create(database.engine)

        # Create the pipeline
        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_change_tracking = pipeline_builder.add_stage('SQL Server Change Tracking Client')
        sql_server_change_tracking.set_attributes(
            table_configs=[{
                'initialOffset': 0,
                'schema': 'dbo',
                'tablePattern': f'{table_name}'
            }]
        )
        wiretap = pipeline_builder.add_wiretap()
        sql_server_change_tracking >> wiretap.destination

        pipeline = pipeline_builder.build("SQL Server CT Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        connection = database.engine.connect()
        # enable change tracking on table
        connection.execute(f'ALTER TABLE {table_name} ENABLE change_tracking WITH (track_columns_updated = on)')

        sdc_executor.start_pipeline(pipeline)

        # Define the data for each statement
        initial_data = {'name': 'Azurill', 'pokedex_id': 298, 'type': 'Normal', 'generation': 3}
        updated_data = {'name': 'Azurill', 'pokedex_id': 298, 'type': 'Normal/Fairy', 'generation': 6}

        # Insert some data and update it
        connection.execute(f"""
            insert into {table_name}
            values (
                '{initial_data.get("name")}',
                {initial_data.get("pokedex_id")},
                '{initial_data.get("type")}',
                {initial_data.get("generation")}
            )
        """)

        # In order to ensure all changes are tracked, a pause is added between changes so no record is lost
        sleep(5)

        connection.execute(f"""
            update {table_name}
            set type = '{updated_data.get("type")}', generation = {updated_data.get("generation")}
            where name = '{updated_data.get("name")}' and pokedex_id = {updated_data.get("pokedex_id")}
        """)

        sleep(5)

        connection.execute(f"delete from {table_name}")

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3)
        assert len(wiretap.output_records) == 3

        primary_key_before_prefix = "jdbc.primaryKey.before."
        primary_key_after_prefix = "jdbc.primaryKey.after."

        for index in range(0, 3):
            header_values = wiretap.output_records[index].header.values

            assert primary_key_before_prefix + "type" not in header_values
            assert primary_key_before_prefix + "generation" not in header_values
            assert primary_key_after_prefix + "type" not in header_values
            assert primary_key_after_prefix + "generation" not in header_values

            if index == 1:
                assert header_values['sdc.operation.type'] == '3'
                assert header_values['jdbc.SYS_CHANGE_OPERATION'] == 'U'

                assert primary_key_before_prefix + "name" in header_values
                assert primary_key_before_prefix + "pokedex_id" in header_values
                assert primary_key_after_prefix + "name" in header_values
                assert primary_key_after_prefix + "pokedex_id" in header_values

                assert header_values[primary_key_before_prefix + "name"] is not None
                assert header_values[primary_key_before_prefix + "pokedex_id"] is not None
                assert header_values[primary_key_after_prefix + "name"] is not None
                assert header_values[primary_key_after_prefix + "pokedex_id"] is not None

                assert header_values[f"{primary_key_before_prefix}name"] == initial_data.get("name")
                assert header_values[f"{primary_key_before_prefix}pokedex_id"] == f'{initial_data.get("pokedex_id")}'
                assert header_values[f"{primary_key_after_prefix}name"] == updated_data.get("name")
                assert header_values[f"{primary_key_after_prefix}pokedex_id"] == f'{updated_data.get("pokedex_id")}'
            else:
                if index == 0:
                    assert header_values['sdc.operation.type'] == '1'
                    assert header_values['jdbc.SYS_CHANGE_OPERATION'] == 'I'
                else:
                    assert header_values['sdc.operation.type'] == '2'
                    assert header_values['jdbc.SYS_CHANGE_OPERATION'] == 'D'

                assert primary_key_before_prefix + "name" not in header_values
                assert primary_key_before_prefix + "pokedex_id" not in header_values
                assert primary_key_after_prefix + "name" not in header_values
                assert primary_key_after_prefix + "pokedex_id" not in header_values

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f'drop table if exists {table_name}')

        if pipeline and (sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING'):
            sdc_executor.stop_pipeline(pipeline)


@sdc_min_version('5.2.0')
@database('sqlserver')
@pytest.mark.parametrize('values', ['numeric', 'non-numeric'])
def test_primary_keys_metadata(sdc_builder, sdc_executor, database, values):
    """
    Test to check the metadata of the primary keys is correctly set in the headers of the output records.
    """
    if not database.is_ct_enabled:
        pytest.skip('Test only runs against SQL Server with CT enabled.')

    pipeline = None
    table_name = get_random_string(string.ascii_lowercase, 20)

    try:
        connection = database.engine.connect()

        if values == 'numeric':
            connection.execute(get_create_table_query_numeric(table_name, database))
        else:
            connection.execute(get_create_table_query_non_numeric(table_name, database))

        # Create the pipeline
        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_change_tracking = pipeline_builder.add_stage('SQL Server Change Tracking Client')
        sql_server_change_tracking.set_attributes(
            table_configs=[{
                'initialOffset': 0,
                'schema': 'dbo',
                'tablePattern': f'{table_name}'
            }]
        )
        wiretap = pipeline_builder.add_wiretap()
        sql_server_change_tracking >> wiretap.destination

        pipeline = pipeline_builder.build("SQL Server CT Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        connection = database.engine.connect()
        # enable change tracking on table
        connection.execute(f'ALTER TABLE {table_name} ENABLE change_tracking WITH (track_columns_updated = on)')
        sdc_executor.start_pipeline(pipeline)

        if values == 'numeric':
            connection.execute(get_insert_query_numeric(table_name, database))
            primary_key_specification_expected = PRIMARY_KEY_NUMERIC_METADATA_SQLSERVER
        else:
            connection.execute(get_insert_query_non_numeric(table_name, database))
            primary_key_specification_expected = PRIMARY_KEY_NON_NUMERIC_METADATA_SQLSERVER

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)

        assert len(wiretap.output_records) == 1

        record = wiretap.output_records[0]
        assert "jdbc.primaryKeySpecification" in record.header.values
        assert record.header.values["jdbc.primaryKeySpecification"] is not None

        primary_key_specification_json = json.dumps(
            json.loads(record.header.values["jdbc.primaryKeySpecification"]),
            sort_keys=True
        )

        primary_key_specification_expected_json = json.dumps(
            json.loads(primary_key_specification_expected),
            sort_keys=True
        )

        assert primary_key_specification_json == primary_key_specification_expected_json

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f'drop table if exists {table_name}')

        if pipeline and (sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING'):
            sdc_executor.stop_pipeline(pipeline)
