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

import logging
import string

import pytest
import sqlalchemy
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.sdc_min_version('5.4.0'), pytest.mark.database('mysql'), pytest.mark.skip]

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Ghastly'},
    {'id': 2, 'name': 'Haunter'},
    {'id': 3, 'name': 'Gengar'}
]
RAW_DATA = ['name'] + [row['name'] for row in ROWS_IN_DATABASE]


# SDC-14882: MySQL Query Consumer closing the connection after each batch
@pytest.mark.parametrize('batch_size', [1, 3, 10])
def test_mysql_consumer_non_incremental_mode(sdc_builder, sdc_executor, database, batch_size):
    """Ensure that the Query consumer works properly in non-incremental mode.

    We test the stage with different batch sizes, since the logic is different depending on the whole table
    fit in a batch or not (see e.g. SDC-14882 for an issue with the second case).

    Pipeline:  mysql_consumer >> wiretap
               mysql_consumer >= finisher

    """

    num_records = 8
    input_data = [{'id': i, 'name': get_random_string()} for i in range(1, num_records + 1)]
    table_name = get_random_string(string.ascii_lowercase, 20)
    sql_query = f'SELECT * FROM {table_name} ORDER BY id ASC'

    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('MySQL Query Consumer')
    origin.set_attributes(incremental_mode=False,
                          sql_query=sql_query,
                          max_batch_size_in_records=batch_size)
    wiretap = pipeline_builder.add_wiretap()
    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")

    origin >> wiretap.destination
    origin >= finisher
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create and populate table
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table = sqlalchemy.Table(table_name,
                                 sqlalchemy.MetaData(),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                                 sqlalchemy.Column('name', sqlalchemy.String(32)))
        table.create(database.engine)
        connection = database.engine.connect()
        connection.execute(table.insert(), input_data)

        # Run the pipeline and check the stage consumed all the expected records. Repeat several times to
        # ensure non-incremental mode works as expected after restarting the pipeline.
        for i in range(3):
            wiretap.reset()
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

            sdc_records = [record.field
                           for record in wiretap.output_records]
            for data in input_data:
                assert data in sdc_records

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


def test_stored_procedure_mysql(sdc_builder, sdc_executor, database, keep_data):
    table_name = get_random_string(string.ascii_lowercase, 20)
    procedure_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name} (
                id int
            )
        """)

        # Create table
        connection.execute(f"""
            INSERT INTO {table_name} VALUES (1)
        """)

        # Create stored procedure
        connection.execute(f"""
            CREATE PROCEDURE {procedure_name}()
            BEGIN
                SELECT * FROM {table_name};
            END;
        """)

        builder = sdc_builder.get_pipeline_builder()
        origin = builder.add_stage('MySQL Query Consumer')
        origin.incremental_mode = False
        origin.sql_query = f"CALL {procedure_name}()"

        wiretap = builder.add_wiretap()

        finisher = builder.add_stage("Pipeline Finisher Executor")
        finisher.preconditions = ['${record:eventType() == \'no-more-data\'}']

        origin >> wiretap.destination
        origin >= finisher

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == 1
        assert records[0].field['id'] == 1

    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            connection.execute(f"DROP PROCEDURE IF EXISTS {procedure_name}")