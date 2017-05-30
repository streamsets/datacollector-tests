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

import logging
import random
import sqlalchemy

from testframework.markers import *
from testframework.utils import get_random_string
from string import ascii_letters

logger = logging.getLogger(__name__)

ROWS_IN_DATABASE = [
    {'name': 'Dima'},
    {'name': 'Jarcec'},
    {'name': 'Arvind'}
]

"""
Check if Jdbc Multi-table Origin can retrieve any records from a table.
Destination is Trash.
Verify input and output (via snapshot).
"""
@database
def test_jdbc_multitable_consumer_origin_simple(sdc_builder, sdc_executor, database):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    trash = pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table_name = get_random_string(ascii_letters, 20)
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
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline,
                                                 start_pipeline=True).wait_for_finished().snapshot
        sdc_executor.stop_pipeline(pipeline)

        rows_from_snapshot = [{record.value['value'][1]['sqpath'].lstrip('/'):
                                   record.value['value'][1]['value']}
                              for record in snapshot[pipeline[0].instance_name].output]

        assert rows_from_snapshot == ROWS_IN_DATABASE
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)

"""
This test case uses the JDBC Query origin.
Test that Pipeline Finisher works.
"""
@database
def test_jdbc_query_no_more_data(sdc_builder, sdc_executor, database):
    metadata = sqlalchemy.MetaData()
    table_name = get_random_string(ascii_letters, 20)
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32))
    )

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_query_consumer = pipeline_builder.add_stage('JDBC Query Consumer')
    jdbc_query_consumer.configuration['isIncrementalMode'] = False
    jdbc_query_consumer.sql_query = 'SELECT * FROM {0};'.format(table_name)

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

"""
Test reading with Multi-table JDBC, output to trash.
Test some table names that start with numbers (SDC-5381).
Check if Pipeline Finished Executor works correctly.
"""
@database
def test_jdbc_multitable_consumer_with_finisher(sdc_builder, sdc_executor, database):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    trash = pipeline_builder.add_stage('Trash')

    jdbc_multitable_consumer >= finisher
    jdbc_multitable_consumer >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)

    random.seed()

    tables = []
    metadata = sqlalchemy.MetaData()
    try:
        connection = database.engine.connect()

        NUMLETTERS = 10
        NUMRECS = 10
        NUMTABLES = 3
        for i in range(0, NUMTABLES):
            if i % 2 == 1:
                # table name starts with a number, contains mixed-case letters.
                input_name = str(i) + get_random_string(ascii_letters, NUMLETTERS)
            else:
                # table name comprised of mixed-case letters only.
                input_name = get_random_string(ascii_letters, NUMLETTERS)

            tables.append(sqlalchemy.Table(
                input_name,
                metadata,
                sqlalchemy.Column('serial', sqlalchemy.Integer, autoincrement=True, primary_key=True),
                sqlalchemy.Column('data', sqlalchemy.Integer)
            ))
            tables[i].create(database.engine)

            rows = [{'data': random.randint(0, 2100000000)} for _ in range(NUMRECS)]
            connection.execute(tables[i].insert(), rows)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

    finally:
        for table in tables:
            table.drop(database.engine)
