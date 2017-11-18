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
import random
import string
import uuid
from time import sleep

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
def test_jdbc_multitable_consumer_origin_default(sdc_builder, database, benchmark, number_of_rows):
    """Performance benchmark a simple JDBC mutli-table consumer to trash pipeline."""
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, 20))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configuration=[{"tablePattern": f'{src_table_prefix}%'}])

    trash = pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >> trash

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
        connection.execute(table.insert(),
                           [{'id': i, 'name': str(uuid.uuid4())} for i in range(1, number_of_rows+1)])

        def benchmark_pipeline(executor, pipeline):
            pipeline.id = str(uuid.uuid4())
            executor.add_pipeline(pipeline)
            executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(number_of_rows, timeout_sec=3600)
            executor.stop_pipeline(pipeline).wait_for_stopped()
            executor.remove_pipeline(pipeline)

        benchmark.pedantic(benchmark_pipeline, args=(sdc_builder, pipeline), rounds=2)
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@sdc_min_version('2.7.0.0')
@pytest.mark.parametrize('number_of_threads', (2, 4, 8, 16))
@pytest.mark.parametrize('number_of_rows', (500_000, 1_000_000, 5_000_000))
@database
def test_jdbc_multitable_consumer_origin_multithreaded(sdc_builder, database, benchmark,
                                                       number_of_rows, number_of_threads):
    """Performance benchmark a simple JDBC mutli-table consumer to trash pipeline."""
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, 20))
    partition_size = str(int(number_of_rows / number_of_threads))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configuration=[{'tablePattern': f'{src_table_prefix}%',
                                                                  'partitionSize': partition_size}],
                                            no_of_threads=number_of_threads,
                                            max_pool_size=number_of_threads)

    trash = pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >> trash

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
        connection.execute(table.insert(),
                           [{'id': i, 'name': str(uuid.uuid4())} for i in range(1, number_of_rows+1)])

        def benchmark_pipeline(executor, pipeline):
            pipeline.id = str(uuid.uuid4())
            executor.add_pipeline(pipeline)
            executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(number_of_rows, timeout_sec=3600)
            executor.stop_pipeline(pipeline).wait_for_stopped()
            executor.remove_pipeline(pipeline)

        benchmark.pedantic(benchmark_pipeline, args=(sdc_builder, pipeline), rounds=2)
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@sdc_min_version('2.7.0.0')
@pytest.mark.parametrize('number_of_rows', (500_000, 1_000_000, 5_000_000))
@database
def test_jdbc_multitable_consumer_origin_partitioning_disabled(sdc_builder, database, benchmark, number_of_rows):
    """Performance benchmark a simple JDBC mutli-table consumer to trash pipeline."""
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, 20))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configuration=[{'tablePattern': f'{src_table_prefix}%',
                                                                  'partitioningMode': 'DISABLED'}])

    trash = pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >> trash

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
        connection.execute(table.insert(),
                           [{'id': i, 'name': str(uuid.uuid4())} for i in range(1, number_of_rows+1)])

        def benchmark_pipeline(executor, pipeline):
            pipeline.id = str(uuid.uuid4())
            executor.add_pipeline(pipeline)
            executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(number_of_rows, timeout_sec=3600)
            executor.stop_pipeline(pipeline).wait_for_stopped()
            executor.remove_pipeline(pipeline)

        benchmark.pedantic(benchmark_pipeline, args=(sdc_builder, pipeline), rounds=2)
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)
