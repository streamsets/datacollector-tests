# Copyright 2017 StreamSets Inc.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The tests in this module follow a pattern of creating pipelines with
:py:obj:`testframework.sdc_models.PipelineBuilder` in one version of SDC and then importing and running them in
another.
"""

import logging
import random
import string

import sqlalchemy

from testframework.environments.databases import oraclize_config_if_needed, upper_if_required
from testframework.markers import database
from testframework.utils import get_random_string

logger = logging.getLogger(__name__)

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Dima'},
    {'id': 2, 'name': 'Jarcec'},
    {'id': 3, 'name': 'Arvind'}
]


@database
def test_jdbc_multitable_consumer_origin_simple(sdc_builder, sdc_executor, database):
    """
    Check if Jdbc Multi-table Origin can retrieve any records from a table.
    Destination is Trash.
    Verify input and output (via snapshot).
    """
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, 20))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    table_config = oraclize_config_if_needed({"tablePattern": f'{src_table_prefix}%'}, database)
    jdbc_multitable_consumer.set_attributes(table_configuration=[table_config])

    trash = pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32))
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        rows_from_snapshot = [{record.value['value'][1]['sqpath'].lstrip('/'):
                                   record.value['value'][1]['value']}
                              for record in snapshot[pipeline[0].instance_name].output]

        assert rows_from_snapshot == [{upper_if_required('name', database): row['name']} for row in ROWS_IN_DATABASE]
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_query_no_more_data(sdc_builder, sdc_executor, database):
    """
    This test case uses the JDBC Query origin.
    Test that Pipeline Finisher works.
    """
    metadata = sqlalchemy.MetaData()
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32))
    )

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_query_consumer = pipeline_builder.add_stage('JDBC Query Consumer')
    jdbc_query_consumer.configuration['isIncrementalMode'] = False
    jdbc_query_consumer.sql_query = 'SELECT * FROM {0}'.format(table_name)

    trash = pipeline_builder.add_stage('Trash')
    jdbc_query_consumer >> trash

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    jdbc_query_consumer >= finisher

    try:
        logger.info('Jdbc No More Data: Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

    finally:
        logger.info('Jdbc No More Data: Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@database
def test_jdbc_multitable_consumer_with_finisher(sdc_builder, sdc_executor, database):
    """
    Test reading with Multi-table JDBC, output to trash.
    Test some table names that start with numbers (SDC-5381).
    Check if Pipeline Finished Executor works correctly.
    """
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    table_config = oraclize_config_if_needed({"tablePattern": f'%{src_table_prefix}%'}, database)
    jdbc_multitable_consumer.set_attributes(table_configuration=[table_config])
    finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    trash = pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >= finisher
    jdbc_multitable_consumer >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    random.seed()

    tables = []
    metadata = sqlalchemy.MetaData()
    try:
        connection = database.engine.connect()

        num_letters = 10
        num_recs = 10
        num_tables = 3
        for i in range(0, num_tables):
            if i % 2 == 1:
                # table name starts with a number, contains mixed-case letters.
                input_name = '{}_{}_{}'.format(str(i), src_table_prefix,
                                               get_random_string(string.ascii_lowercase, num_letters))
            else:
                # table name comprised of mixed-case letters only.
                input_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, num_letters))

            tables.append(sqlalchemy.Table(
                input_name,
                metadata,
                sqlalchemy.Column('serial', sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column('data', sqlalchemy.Integer)
            ))
            tables[i].create(database.engine)

            rows = [{'serial': j, 'data': random.randint(0, 2100000000)} for j in range(1, num_recs + 1)]
            connection.execute(tables[i].insert(), rows)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
    finally:
        for table in tables:
            table.drop(database.engine)
