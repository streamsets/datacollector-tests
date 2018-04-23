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
The tests in this module are for running JDBC based pipelines to assert on system fault resilience.
"""

import logging
import string
import uuid
from time import sleep

import pytest
import sqlalchemy
from streamsets.testframework.markers import database
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@database
def test_query_consumer_network(sdc_builder, sdc_executor, database):
    """Test simple JDBC query consumer origin for network fault tolerance. We delay the pipeline using a Delay stage
    so as we get time to shut the SDC container network to test retry and resume logic of origin stage.
    The pipeline looks like:
        jdbc_query_consumer >> delay >> trash
        jdbc_query_consumer >= finisher
    """
    number_of_rows = 10_000
    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_query_consumer = pipeline_builder.add_stage('JDBC Query Consumer')
    jdbc_query_consumer.set_attributes(incremental_mode=False, sql_query=f'SELECT * FROM {table_name}')

    delay = pipeline_builder.add_stage('Delay')
    # milliseconds to delay between batches, so as we get time to disconnect network
    delay.set_attributes(delay_between_batches=1000)

    trash = pipeline_builder.add_stage('Trash')

    finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')

    jdbc_query_consumer >> delay >> trash
    jdbc_query_consumer >= finisher

    pipeline = pipeline_builder.build('JDBC Query Origin').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table_name, metadata,
                             sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                             sqlalchemy.Column('name', sqlalchemy.String(40)))
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding %s rows into %s database ...', number_of_rows, database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), [{'id': i, 'name': str(uuid.uuid4())} for i in range(1, number_of_rows+1)])

        pipeline_cmd = sdc_executor.start_pipeline(pipeline)
        pipeline_cmd.wait_for_pipeline_output_records_count(int(number_of_rows/3))
        sdc_executor.container.network_disconnect()
        sleep(5) # sleep few seconds to have pipeline go into retry mode
        sdc_executor.container.network_reconnect()
        pipeline_cmd.wait_for_finished()

        history = sdc_executor.get_pipeline_history(pipeline)
        # -2 to take out two events generated from record count
        pipeline_record_count = (history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count - 2)
        assert pipeline_record_count == number_of_rows
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)
