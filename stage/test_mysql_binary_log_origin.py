# Copyright 2020 StreamSets Inc.
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

import time
import logging
import string

import pytest
import sqlalchemy
from streamsets.testframework.environments.databases import MySqlDatabase
from streamsets.testframework.markers import database
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def cdc_check(database):
    if isinstance(database, MySqlDatabase) and not database.is_cdc_enabled:
            pytest.skip('Test only runs against MySQL with CDC enabled.')


@database('mysql')
def test_mysql_binary_log_json_column(sdc_builder, sdc_executor, database, keep_data):
    """Test that MySQL Binary Log Origin is able to correctly read a json column in a row coming from MySQL Binary Log
    (AKA CDC).

    Pipeline looks like:

        mysql_binary_log >> trash
    """
    table = None
    connection = None

    try:
        # Create table.
        connection = database.engine.connect()
        table_name = get_random_string(string.ascii_lowercase, 20)
        table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, autoincrement=False),
                                 sqlalchemy.Column('name', sqlalchemy.String(25)),
                                 sqlalchemy.Column('json_column', sqlalchemy.JSON))
        table.create(database.engine)

        # Insert data into table.
        connection.execute(table.insert(), {'id': 100, 'name': 'a', 'json_column': {'a': 123, 'b': 456}})

        # Create Pipeline.
        pipeline_builder = sdc_builder.get_pipeline_builder()
        mysql_binary_log = pipeline_builder.add_stage('MySQL Binary Log')
        mysql_binary_log.set_attributes(start_from_beginning=True,
                                        server_id='1',
                                        include_tables=database.database + '.' + table_name)
        trash = pipeline_builder.add_stage('Trash')

        mysql_binary_log >> trash

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Run pipeline and verify output.
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batches=1).snapshot
        sdc_executor.stop_pipeline(pipeline)

        for record in snapshot.snapshot_batches[0][mysql_binary_log.instance_name].output:
            assert record.field['Data']['id'] == 100
            assert record.field['Data']['name'] == 'a'
            assert record.field['Data']['json_column'].value == '{"a":123,"b":456}'

    finally:
        if not keep_data:
            # Drop table and Connection.
            if table is not None:
                logger.info('Dropping table %s in %s database...', table, database.type)
                table.drop(database.engine)

            if connection is not None:
                connection.close()


@database('mysql')
def test_mysql_bin_log_stop_resume(sdc_builder, sdc_executor, database, keep_data):
    """Test that MySQL Binary Log Origin is able to resume offset after one run reading information in both runs

    Pipeline looks like:
        mysql_binary_log >> trash
    """
    connection = None

    table_name = get_random_string(string.ascii_lowercase, 20)
    sample_data = [f'Martin_{i}' for i in range(20)]

    table = sqlalchemy.Table(table_name,
                             sqlalchemy.MetaData(),
                             sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                             sqlalchemy.Column('name', sqlalchemy.String(20)))

    # Create Pipeline.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    mysql_binary_log = pipeline_builder.add_stage('MySQL Binary Log')
    mysql_binary_log.set_attributes(start_from_beginning=True,
                                    server_id='1',
                                    include_tables=f'{database.database}.{table_name}')
    trash = pipeline_builder.add_stage('Trash')

    mysql_binary_log >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        connection = database.engine.connect()

        table.create(database.engine)

        with database.engine.connect().execution_options(autocommit=True) as connection:
            for row in sample_data[:10]:
                connection.execute(f'INSERT INTO {table_name} (name) VALUES (\'{row}\')')

        # Run pipeline and verify output.
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert [record.field['Data']['name']
                for record in snapshot.snapshot_batches[0][mysql_binary_log.instance_name].output] == sample_data[:10]

        with database.engine.connect().execution_options(autocommit=True) as connection:
            for row in sample_data[10:20]:
                connection.execute(f'INSERT INTO {table_name} (name) VALUES (\'{row}\')')

        # Run pipeline and verify output.
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert [record.field['Data']['name']
                for record in snapshot.snapshot_batches[0][mysql_binary_log.instance_name].output] == sample_data[10:20]

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)

        if not keep_data:
            # Drop table and Connection.
            if table is not None:
                logger.info('Dropping table %s in %s database...', table, database.type)
                table.drop(database.engine)

            if connection is not None:
                connection.close()


# SDC-15872: Disconnect MySQL Bin Log client if if it's not connected
@database('mysql')
def test_disconnect_on_error(sdc_builder, sdc_executor, database, keep_data):
    """Verify that we properly disconnect from the database on error (such as second slave with the same id)."""
    table = None
    connection = None

    try:
        # Create table.
        connection = database.engine.connect()
        table_name = get_random_string(string.ascii_lowercase, 20)
        table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=False, autoincrement=False, quote=True),
                                 quote=True)
        table.create(database.engine)
        connection.execute(table.insert(), {'id': 1})

        # We need two pipelines that are using the same server id
        builder = sdc_builder.get_pipeline_builder()
        origin = builder.add_stage('MySQL Binary Log')
        origin.set_attributes(start_from_beginning=True,
                              server_id='1',
                              include_tables=database.database + '.' + table_name)
        wiretap1 = builder.add_wiretap()
        origin >> wiretap1.destination
        pipeline1 = builder.build().configure_for_environment(database)
        pipeline1.configuration['shouldRetry'] = False
        sdc_executor.add_pipeline(pipeline1)

        # Create second pipeline (same logical pipeline though)
        builder = sdc_builder.get_pipeline_builder()
        origin = builder.add_stage('MySQL Binary Log')
        origin.set_attributes(start_from_beginning=True,
                              server_id='1',
                              include_tables=database.database + '.' + table_name)
        wiretap2 = builder.add_wiretap()
        origin >> wiretap2.destination
        pipeline2 = builder.build().configure_for_environment(database)
        pipeline2.configuration['shouldRetry'] = False
        sdc_executor.add_pipeline(pipeline2)

        # 1) Start pipeline 1, wait until it consumes at least that one record
        sdc_executor.start_pipeline(pipeline1).wait_for_pipeline_output_records_count(1)

        # 2) Start pipeline 2 (while pipeline 1 is still running)
        sdc_executor.start_pipeline(pipeline2)

        # 3) Pipeline 1 should at this point fail (two different slaves with the same id)
        time.sleep(5)
        assert sdc_executor.get_pipeline_status(pipeline1).response.json().get('status') in ("RUN_ERROR", "RUNNING_ERROR")

        # 4) Pipeline 2 can be stopped at this point
        sdc_executor.stop_pipeline(pipeline2)

        # 5) We start the first pipeline and insert data in 5-second interval, the pipeline should read all of them
        # In order to see the problem with the left-over thread, we need to wait at least a minute.
        sdc_executor.start_pipeline(pipeline1)
        for i in range(2, 10):
            time.sleep(10)
            logger.info(f"Inserting {i}")
            connection.execute(table.insert(), {'id': i})

        # 6) Finally the pipeline should still be running and should process all 10 rows. Here is where
        # a version without SDC-15872 should fail.
        assert sdc_executor.get_pipeline_status(pipeline1).response.json().get('status') == "RUNNING"

        # 7) Stop pipeline 1 and validate that we got all the records into the wiretap
        sdc_executor.stop_pipeline(pipeline1)

        records = wiretap1.output_records
        assert len(records) == 9

        for i in range(1, 10):
            assert records[i-1].field['Data']['id'] == i
    finally:
        if not keep_data:
            # Drop table and Connection.
            if table is not None:
                logger.info('Dropping table %s in %s database...', table, database.type)
                table.drop(database.engine)

            if connection is not None:
                connection.close()