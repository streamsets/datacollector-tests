# Copyright 20202 StreamSets Inc.
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
import math
import string

import pytest
import sqlalchemy
from streamsets.testframework.markers import credentialstore, database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Dima'},
    {'id': 2, 'name': 'Jarcec'},
    {'id': 3, 'name': 'Arvind'}
]
ROWS_TO_UPDATE = [
    {'id': 2, 'name': 'Eddie'},
    {'id': 4, 'name': 'Jarcec'}
]
LOOKUP_RAW_DATA = ['id'] + [str(row['id']) for row in ROWS_IN_DATABASE]
RAW_DATA = ['name'] + [row['name'] for row in ROWS_IN_DATABASE]

DEFAULT_DB2_SCHEMA = 'DB2INST1'


# SDC-14882: JDBC Query Consumer closing the connection after each batch
@database
@pytest.mark.parametrize('batch_size', [1, 3, 10])
def test_jdbc_consumer_non_incremental_mode(sdc_builder, sdc_executor, database, batch_size):
    """Ensure that the Query consumer works properly in non-incremental mode.

    We test the stage with different batch sizes, since the logic is different depending on the whole table
    fit in a batch or not (see e.g. SDC-14882 for an issue with the second case).

    Pipeline:  jdbc_consumer >> trash
               jdbc_consumer >= finisher

    """
    if database.type == 'Oracle':
        pytest.skip("This test depends on proper case for column names that Oracle auto-uppers.")

    num_records = 8
    input_data = [{'id': i, 'name': get_random_string()} for i in range(1, num_records + 1)]
    table_name = get_random_string(string.ascii_lowercase, 20)
    sql_query = f'SELECT * FROM {table_name} ORDER BY id ASC'

    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('JDBC Query Consumer')
    origin.set_attributes(incremental_mode=False,
                          sql_query=sql_query,
                          max_batch_size_in_records=batch_size)
    trash = pipeline_builder.add_stage('Trash')
    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")

    origin >> trash
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
            snapshot = sdc_executor.capture_snapshot(pipeline=pipeline,
                                                     start_pipeline=True,
                                                     batches=math.ceil(num_records / batch_size),
                                                     batch_size=batch_size).snapshot
            sdc_records = [record.field
                           for batch in snapshot.snapshot_batches
                           for record in batch[origin.instance_name].output]
            assert sdc_records == input_data
            sdc_executor.get_pipeline_status(pipeline).wait_for_status('FINISHED')

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)

