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
import re
import string
from time import sleep, time

import pytest
import sqlalchemy

from testframework.markers import database
from testframework.utils import get_random_string

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# lower case tables have better compatibility with databases (e.g., PostgreSQL)
EVENT_COLUMN_NAME = 'event_status'
PRIMARY_KEY = 'pid'
OTHER_COLUMN = 'randomstring'
NO_OF_SRC_ROWS = 60
PARTITION_SIZE = '10'
# lowercase for db compatibility (e.g. PostgreSQL)
SRC_TABLE_PREFIX = get_random_string(string.ascii_lowercase, 6)
TGT_TABLE_PREFIX = get_random_string(string.ascii_lowercase, 6)

TABLE_PREFIX_NAME_FMT = '{table_prefix}_{table_name}'


def assert_tables_replicated(database=None, src_table_names=None):
    """Goes through all source tables and checks the corresponding mapping to a target table."""
    db_engine = database.engine
    for src_table_name in src_table_names:
        target_table_name = re.sub(SRC_TABLE_PREFIX, TGT_TABLE_PREFIX, src_table_name, 1)
        logger.info('Comparing Source Table : %s and Target Table : %s', src_table_name, target_table_name)

        src_table = sqlalchemy.Table(src_table_name, sqlalchemy.MetaData(), autoload=True, autoload_with=db_engine)
        src_result = db_engine.execute(src_table.select().order_by(src_table.c[PRIMARY_KEY]))
        src_result_list = src_result.fetchall()
        src_result.close()

        target_table = sqlalchemy.Table(target_table_name, sqlalchemy.MetaData(),
                                        autoload=True, autoload_with=db_engine)
        target_result = db_engine.execute(target_table.select().order_by(target_table.c[PRIMARY_KEY]))
        target_result_list = target_result.fetchall()
        target_result.close()

        assert src_result_list == target_result_list


def wait_for_no_data_event(event_table_name, database=None, timeout_sec=240):
    """Wait for no_data_event update on EVENT_TABLE (i.e, event column should be set to 1)."""
    logger.info('Waiting for no_data_event to be updated in %s seconds ...', timeout_sec)
    start_waiting_time = time()
    stop_waiting_time = start_waiting_time + timeout_sec
    db_engine = database.engine

    while time() < stop_waiting_time:
        event_table = sqlalchemy.Table(event_table_name, sqlalchemy.MetaData(), autoload=True, autoload_with=db_engine)
        event_result = db_engine.execute(sqlalchemy.select([event_table.c[EVENT_COLUMN_NAME]]))
        event_result_list = event_result.fetchall()
        event_result.close()

        if event_result_list[0][0] == 1:
            logger.info('Received NO_MORE_DATA_EVENT')
            return
        sleep(5)

    raise Exception('Timed out after %s seconds while waiting for no data event.')


def setup_tables(database, src_table_names, target_table_names, event_table_name):
    """Creates source, target and event tables, inserts rows to the source table and
    insert 0 for event table's event column.
    """
    db_engine = database.engine
    for table_name in src_table_names:
        logger.info('Creating source table %s in %s database ...', table_name, database.type)
        table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                 sqlalchemy.Column(PRIMARY_KEY, sqlalchemy.Integer, primary_key=True),
                                 sqlalchemy.Column(OTHER_COLUMN, sqlalchemy.String(20)))
        table.create(db_engine)
        logger.info('Inserting data into source table %s in %s database ...', table_name, database.type)
        for src_row_id in range(1, NO_OF_SRC_ROWS+1):  # some databases (like MySQL) will start from 1
            db_engine.execute(table.insert(), [{PRIMARY_KEY: src_row_id,
                                                OTHER_COLUMN: get_random_string(string.ascii_lowercase, 20)}])

    for table_name in target_table_names:
        logger.info('Creating target table %s in %s database ...', table_name, database.type)
        table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                 sqlalchemy.Column(PRIMARY_KEY, sqlalchemy.Integer, primary_key=True),
                                 sqlalchemy.Column(OTHER_COLUMN, sqlalchemy.String(20)))
        table.create(db_engine)

    logger.info('Creating event table %s in %s database ...', event_table_name, database.type)
    table = sqlalchemy.Table(event_table_name, sqlalchemy.MetaData(),
                             sqlalchemy.Column(EVENT_COLUMN_NAME, sqlalchemy.Integer))
    table.create(db_engine)
    logger.info('Inserting data into event table %s in %s database ...', event_table_name, database.type)
    db_engine.execute(table.insert(), [{EVENT_COLUMN_NAME: 0}])


def teardown_tables(database, table_names):
    """Drops both source, target tables and event table."""
    db_engine = database.engine
    for table_name in table_names:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(), autoload=True, autoload_with=db_engine)
        table.drop(db_engine)


@database
# lowercase for db compatibility (e.g. PostgreSQL)
@pytest.mark.parametrize('table_name_characters', [string.ascii_lowercase, string.digits])
@pytest.mark.parametrize('table_name_length', [14])
@pytest.mark.parametrize('no_of_tables', [9])
@pytest.mark.parametrize('no_of_threads', [1, 5])
@pytest.mark.parametrize('batch_strategy', ["SWITCH_TABLES", "PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE"])
@pytest.mark.parametrize('partitioning_mode', ["DISABLED", "BEST_EFFORT"])
@pytest.mark.timeout(300)
def test_jdbc_multitable_consumer_to_jdbc(sdc_builder, sdc_executor, database,
                                          table_name_characters,
                                          table_name_length,
                                          no_of_tables,
                                          no_of_threads,
                                          batch_strategy,
                                          partitioning_mode):
    """Tests Multithreaded Multi-table JDBC source. Replicates a set of tables with prefix 'src' to a another
    set of tables with 'target' prefix. Also leveraging the NO_MORE_DATA EVENT by the Multi-table JDBC source after
    no data in tables. On Event path a JDBC Executor updates a special event table to mark data read to stop pipeline.
    The pipeline would look like:

            jdbc_multitable_consumer >> jdbc_query_dest
                                     >= jdbc_query_event
    """
    event_table_name = get_random_string(string.ascii_lowercase, 10)
    update_event_table_statement = f'UPDATE {event_table_name} set {EVENT_COLUMN_NAME} = 1'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(no_of_threads=no_of_threads, batch_strategy=batch_strategy,
                                            max_pool_size=no_of_threads, min_idle_connections=no_of_threads,
                                            table_configuration=[{"tablePattern": f'{SRC_TABLE_PREFIX}%',
                                                                  'partitioningMode': partitioning_mode,
                                                                  'partitionSize': PARTITION_SIZE}])

    if partitioning_mode == 'BEST_EFFORT':
        # for partitioning, increase the max allowed queries per second (will be changed as SDC default via SDC-7867)
        jdbc_multitable_consumer.configuration['commonSourceConfigBean.queriesPerSecond'] = '50'

    # The target used to replicate is JDBCQueryExecutor.
    # After SDC-5757 is resolved, we can use JDBCProducer.
    jdbc_query_dest = pipeline_builder.add_stage('JDBC Query', type='executor')
    table_name = (f"${{str:replace(record:attribute('jdbc.tables'),"
                  f"'{SRC_TABLE_PREFIX if not database.type == 'Oracle' else SRC_TABLE_PREFIX.upper()}',"
                  f"'{TGT_TABLE_PREFIX}')}}")
    query = (f"INSERT into {table_name} values "
             f"(${{record:value('/{PRIMARY_KEY if not database.type == 'Oracle' else PRIMARY_KEY.upper()}')}}"
             f", '${{record:value('/{OTHER_COLUMN if not database.type == 'Oracle' else OTHER_COLUMN.upper()}')}}')")

    jdbc_query_dest.set_attributes(sql_query=query)
    jdbc_query_event = pipeline_builder.add_stage('JDBC Query', type='executor')
    jdbc_query_event.set_attributes(sql_query=update_event_table_statement)

    jdbc_multitable_consumer >> jdbc_query_dest
    jdbc_multitable_consumer >= jdbc_query_event
    pipeline_name = f'JDBC multitable consumer pipeline - {batch_strategy} batch strategy, {no_of_threads} threads, {partitioning_mode} partitioning'
    pipeline = pipeline_builder.build(pipeline_name).configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    # Generate random table names.
    table_names = [get_random_string(table_name_characters, table_name_length).lower() + '_' + str(tableNo)
                   for tableNo in range(0, no_of_tables)]
    src_table_names = ([TABLE_PREFIX_NAME_FMT.format(table_prefix=SRC_TABLE_PREFIX,
                                                     table_name=table_name)
                        for table_name in table_names])
    target_table_names = ([TABLE_PREFIX_NAME_FMT.format(table_prefix=TGT_TABLE_PREFIX,
                                                        table_name=table_name)
                           for table_name in table_names])
    try:
        setup_tables(database, src_table_names, target_table_names, event_table_name)
        sdc_executor.start_pipeline(pipeline)
        wait_for_no_data_event(event_table_name, database)
        assert_tables_replicated(database, src_table_names)
    finally:
        # must stop pipeline before tables can be dropped.
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Dropping test related tables in %s database...', database.type)
        teardown_tables(database, src_table_names + target_table_names + [event_table_name])
