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

"""
The tests in this module are for running high-volume pipelines, for the purpose of performance testing.
"""

import logging
import string
import uuid

import pytest
import sqlalchemy

from testframework.markers import database, sdc_min_version
from testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def sdc_builder_hook():
    def hook(data_collector):
        data_collector.SDC_JAVA_OPTS = '-Xmx8192m -Xms8192m'
    return hook


@pytest.mark.parametrize('number_of_rows', (500_000, 1_000_000, 5_000_000))
@database
def test_jdbc_query_consumer_origin_default(sdc_builder, sdc_executor, database, benchmark, number_of_rows):
    """Performance benchmark a simple JDBC query consumer to trash pipeline."""
    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_query_consumer = pipeline_builder.add_stage('JDBC Query Consumer')
    jdbc_query_consumer.set_attributes(incremental_mode=False,
                                       sql_query=f'SELECT * FROM {table_name}')

    trash = pipeline_builder.add_stage('Trash')
    jdbc_query_consumer >> trash

    finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    jdbc_query_consumer >= finisher

    pipeline = pipeline_builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table_name,
                             metadata,
                             sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                             sqlalchemy.Column('name', sqlalchemy.String(40)))
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding %s rows into %s database ...', number_of_rows, database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), [{'id': i, 'name': str(uuid.uuid4())} for i in range(1, number_of_rows+1)])

        def benchmark_pipeline(executor, pipeline):
            pipeline.id = str(uuid.uuid4())
            executor.add_pipeline(pipeline)
            executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=3600)
            executor.remove_pipeline(pipeline)

        benchmark.pedantic(benchmark_pipeline, args=(sdc_executor, pipeline), rounds=2)
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)
