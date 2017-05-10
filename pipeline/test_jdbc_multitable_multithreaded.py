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
import re
import string
from os.path import dirname, join, realpath
from time import sleep

from testframework import environment, sdc
from testframework.markers import *
from testframework.utils import get_random_string


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


EVENT_TABLE_NAME = 'SRC_NO_DATAEVENT'
EVENT_COLUMN_NAME = 'EVENT_STATUS'
PRIMARY_KEY = 'pid'
OTHER_COLUMN = 'randomstring'

CREATE_DATA_TABLE_STATEMENT = ('CREATE TABLE `{table_name}` '
                               '({primary_key} INT NOT NULL PRIMARY KEY, {other_col} VARCHAR(20))'
                               .format(table_name='{table_name}',
                                       primary_key=PRIMARY_KEY,
                                       other_col=OTHER_COLUMN))

INSERT_DATA_TABLE_STATEMENT = "INSERT into `{table_name}` values ({primary_key}, '{other_col}')"

SELECT_DATA_TABLE_STATEMENT = ('SELECT * FROM `{table_name}` ORDER BY {primary_key}'.format(
    table_name='{table_name}', primary_key=PRIMARY_KEY))

INSERT_EVENT_TABLE_STATEMENT = ('INSERT INTO `{event_table_name}` values ({event_column_value})'
                                .format(event_table_name=EVENT_TABLE_NAME,
                                        event_column_value='{event_column_value}'))
CREATE_EVENT_TABLE_STATEMENT = ('CREATE TABLE `{event_table_name}` ({event_column} int)'
                                .format(event_table_name=EVENT_TABLE_NAME,
                                        event_column=EVENT_COLUMN_NAME))
SELECT_EVENT_TABLE_STATEMENT = ('SELECT `{event_column_name}` from `{event_table_name}`'
                                .format(event_column_name=EVENT_COLUMN_NAME,
                                        event_table_name=EVENT_TABLE_NAME))
UPDATE_EVENT_TABLE_STATEMENT = ('UPDATE `{event_table_name}` set `{event_column_name}`= 1'
                                .format(event_table_name=EVENT_TABLE_NAME,
                                        event_column_name=EVENT_COLUMN_NAME))

DROP_TABLE_STATEMENT = 'DROP TABLE `{table_name}`'.format(table_name='{table_name}')

NO_OF_SRC_ROWS = 200
SRC_TABLE_PREFIX = 'src'
TGT_TABLE_PREFIX = 'target'

TABLE_PREFIX_NAME_FMT = '{table_prefix}_{table_name}'
JDBC_QUERY_TBL_FORMAT = ("${{str:replace(record:attribute('jdbc.tables'),"
                         "'{src_table_prefix}','{target_table_prefix}')}}")
JDBC_QUERY_OTHER_COL = "${{record:value('/{other_col}')}}"
JDBC_QUERY_PRIMARY_COL = "${{record:value('/{primary_key}')}}"


# Close the opened cursor and if any exceptions happen during closing simply log and move.
def close_cursor_quietly(cursor):
    if cursor:
        try:
            cursor.close()
        except Exception as e:
            # quietly log exception but not fail the test case
            logger.error('Error when closing cursor:  %s', e)


# Execute SQL Statement using db_cursor.
def execute_statement(db_cursor, statement):
    logger.info('Executing Statement %s', statement)
    db_cursor.execute(statement)


# Goes through all source tables and checks the corresponding mapping to a target table.
def assert_tables_replicated(db, src_table_names):
    for src_table_name in src_table_names:
        target_table_name = re.sub(SRC_TABLE_PREFIX, TGT_TABLE_PREFIX, src_table_name, 1)
        logger.info(
            'Comparing Source Table : %s and Target Table : %s',
            src_table_name,
            target_table_name
        )

        db_cursor1 = None
        db_cursor2 = None
        try:
            db_cursor1 = db.connection.cursor()
            db_cursor2 = db.connection.cursor()

            execute_statement(
                db_cursor1,
                SELECT_DATA_TABLE_STATEMENT.format(table_name=src_table_name)
            )
            execute_statement(
                db_cursor2,
                SELECT_DATA_TABLE_STATEMENT.format(table_name=target_table_name)
            )

            src_table_rows = db_cursor1.fetchall()
            target_table_rows = db_cursor2.fetchall()

            assert src_table_rows == target_table_rows
        finally:
            close_cursor_quietly(db_cursor1)
            close_cursor_quietly(db_cursor2)


# Wait for no_data_event update on EVENT_TABLE (i.e, event column should be set to 1).
def wait_for_no_data_event(db):
    while True:
        db_cursor = None
        try:
            db_cursor = db.connection.cursor()
            execute_statement(db_cursor, SELECT_EVENT_TABLE_STATEMENT)
            result = db_cursor.fetchall()
            if result[0][0] == 1:
                logger.info('Received NO_MORE_DATA_EVENT...')
                break
            sleep(20)
        finally:
            close_cursor_quietly(db_cursor)


# Creates source and target tables, inserts rows to the source table.
# Also creates event table and set event column to 0.
def setup_tables(db, src_table_names, target_table_names):
    db_cursor = None
    try:
        db_cursor = db.connection.cursor()
        for table_name in src_table_names:
            execute_statement(db_cursor, CREATE_DATA_TABLE_STATEMENT.format(table_name=table_name))
            for src_row_id in range(NO_OF_SRC_ROWS):
                execute_statement(
                    db_cursor,
                    INSERT_DATA_TABLE_STATEMENT.format(
                        table_name=table_name,
                        primary_key=str(src_row_id),
                        other_col=get_random_string(string.ascii_lowercase, 20))
                )
        for table_name in target_table_names:
            execute_statement(db_cursor, CREATE_DATA_TABLE_STATEMENT.format(table_name=table_name))

        execute_statement(db_cursor, CREATE_EVENT_TABLE_STATEMENT)
        execute_statement(db_cursor, INSERT_EVENT_TABLE_STATEMENT.format(event_column_value=0))
    finally:
        close_cursor_quietly(db_cursor)


# Drops both source, target tables and event table.
def teardown_tables(db, table_names):
    db_cursor = None
    try:
        db_cursor = db.connection.cursor()
        for table_name in table_names:
            execute_statement(db_cursor, DROP_TABLE_STATEMENT.format(table_name=table_name))
    finally:
        close_cursor_quietly(db_cursor)


@database
@pytest.mark.parametrize('table_name_characters', [string.ascii_letters, string.digits])
@pytest.mark.parametrize('table_name_length', [20])
@pytest.mark.parametrize('no_of_tables', [25, 50])
@pytest.mark.parametrize('no_of_threads', [1, 3, 5])
@pytest.mark.parametrize(
    'batch_strategy',
    ["SWITCH_TABLES", "PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE"]
)
@pytest.mark.timeout(300)
# Tests Multithreaded Multitable JDBC source.
# Replicates a set of tables with prefix 'src' to a another set of tables with 'target' prefix.
#
# Also leveraging the NO_MORE_DATA EVENT by the Multitable JDBC source after no data in tables.
# On Event path a JDBC Executor updates a special event table to mark data read to stop pipeline.
def test_jdbc_multitable_consumer_to_jdbc(args,
                                          table_name_characters,
                                          table_name_length,
                                          no_of_tables,
                                          no_of_threads,
                                          batch_strategy):
    db = environment.Database(database=args.database)
    pipeline = sdc.Pipeline(
        join(dirname(realpath(__file__)),
             'pipelines',
             'jdbc_multi_table_to_jdbc.json')
    ).configure_for_environment(db)

    multi_table_stage = pipeline.stages['JDBCMultitableConsumer_01']

    first_table_config = multi_table_stage.table_configuration[0]

    first_table_config['tablePattern'] = ('{src_table_prefix}%'
                                          .format(src_table_prefix=SRC_TABLE_PREFIX))
    multi_table_stage.no_of_threads = no_of_threads
    multi_table_stage.batch_strategy = batch_strategy
    multi_table_stage.configuration['hikariConfigBean.maximumPoolSize'] = no_of_threads
    multi_table_stage.configuration['hikariConfigBean.minIdle'] = no_of_threads

    # The target used to replicate is JDBCQueryExecutor.
    # After SDC-5757 is resolved, we can use JDBCProducer.
    pipeline.stages['JDBCQuery_02'].query = (INSERT_DATA_TABLE_STATEMENT.format(
            table_name=JDBC_QUERY_TBL_FORMAT.format(
                src_table_prefix=SRC_TABLE_PREFIX,
                target_table_prefix=TGT_TABLE_PREFIX
            ),
            primary_key=JDBC_QUERY_PRIMARY_COL.format(primary_key=PRIMARY_KEY),
            other_col=JDBC_QUERY_OTHER_COL.format(other_col=OTHER_COLUMN)
        ))

    pipeline.stages['JDBCQuery_01'].query = UPDATE_EVENT_TABLE_STATEMENT

    # Generate random table names.
    table_names = [get_random_string(table_name_characters, table_name_length) + '_' + str(tableNo)
                   for tableNo in range(0, no_of_tables)]

    src_table_names = ([TABLE_PREFIX_NAME_FMT.format(table_prefix=SRC_TABLE_PREFIX,
                                                     table_name=table_name)
                        for table_name in table_names])
    target_table_names = ([TABLE_PREFIX_NAME_FMT.format(table_prefix=TGT_TABLE_PREFIX,
                                                        table_name=table_name)
                           for table_name in table_names])

    # Setup the tables for test.
    setup_tables(db, src_table_names, target_table_names)

    try:
        with sdc.DataCollector(version=args.sdc_version) as data_collector:
            data_collector.add_pipeline(pipeline)
            data_collector.start()
            data_collector.start_pipeline(pipeline)
            wait_for_no_data_event(db)
            data_collector.stop_pipeline(pipeline).wait_for_stopped()
            assert_tables_replicated(db, src_table_names)
    finally:
        # Drop tables after test.
        teardown_tables(db, src_table_names + target_table_names + [EVENT_TABLE_NAME])

