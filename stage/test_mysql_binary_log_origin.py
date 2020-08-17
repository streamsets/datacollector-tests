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

import logging
import string

import pytest
import sqlalchemy
from streamsets.testframework.markers import database
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@database('mysql')
def test_mysql_binary_log_json_column(sdc_builder, sdc_executor, database):
    """Test that MySQL Binary Log Origin is able to correctly read a json column in a row coming from MySQL Binary Log
    (AKA CDC).

    Pipeline looks like:

        mysql_binary_log >> trash
    """
    table = None
    connection = None

    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against MySQL with CDC enabled.')

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
        # Drop table and Connection.
        if table is not None:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)

        if connection is not None:
            connection.close()


@database('mysql')
def test_mysql_bin_log_stop_resume(sdc_builder, sdc_executor, database):
    """Test that MySQL Binary Log Origin is able to resume offset after one run reading information in both runs

    Pipeline looks like:
        mysql_binary_log >> trash
    """
    connection = None

    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against MySQL with CDC enabled.')

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
        # Drop table and Connection.
        if table is not None:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)

        if connection is not None:
            connection.close()
