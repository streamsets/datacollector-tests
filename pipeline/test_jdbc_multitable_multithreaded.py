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

from collections import namedtuple
import logging
import random
import re
import string
from time import sleep, time

import pytest
import sqlalchemy
from streamsets.sdk.utils import Version
from streamsets.testframework.utils import get_random_string
from streamsets.testframework.markers import database, sdc_min_version

logger = logging.getLogger(__name__)


# lower case tables have better compatibility with databases (e.g., PostgreSQL)
EVENT_COLUMN_NAME = 'event_status'
FIRST_COLUMN = 'pid'
OTHER_COLUMN = 'randomstring'
NO_OF_SRC_ROWS = 60
PARTITION_SIZE = '10'
# lowercase for db compatibility (e.g. PostgreSQL)
SRC_TABLE_PREFIX = get_random_string(string.ascii_lowercase, 6)
TGT_TABLE_PREFIX = get_random_string(string.ascii_lowercase, 6)

TABLE_PREFIX_NAME_FMT = '{table_prefix}_{table_name}'

TableInfo = namedtuple('TableInfo', ['name', 'use_primary_key'])

def assert_tables_replicated(database=None, src_tables=None):
    """Goes through all source tables and checks the corresponding mapping to a target table."""
    db_engine = database.engine
    for src_table_info in src_tables:
        target_table_name = re.sub(SRC_TABLE_PREFIX, TGT_TABLE_PREFIX, src_table_info.name, 1)
        logger.info('Comparing Source Table : %s and Target Table : %s', src_table_info.name, target_table_name)

        src_table = sqlalchemy.Table(src_table_info.name, sqlalchemy.MetaData(), autoload=True, autoload_with=db_engine)
        src_result = db_engine.execute(src_table.select().order_by(src_table.c[FIRST_COLUMN]))
        src_result_list = src_result.fetchall()
        src_result.close()

        target_table = sqlalchemy.Table(target_table_name, sqlalchemy.MetaData(),
                                        autoload=True, autoload_with=db_engine)
        target_result = db_engine.execute(target_table.select().order_by(target_table.c[FIRST_COLUMN]))
        target_result_list = target_result.fetchall()
        target_result.close()

        assert src_result_list == target_result_list

def setup_tables(database, src_tables, target_tables, event_table_name):
    """Creates source, target and event tables, inserts rows to the source table and
    insert 0 for event table's event column.
    """
    db_engine = database.engine

    for src_table in src_tables:
        first_col = sqlalchemy.Column(FIRST_COLUMN, sqlalchemy.Integer, primary_key=src_table.use_primary_key, autoincrement=False)
        logger.info('Creating source table %s in %s database ...', src_table.name, database.type)
        table = sqlalchemy.Table(src_table.name, sqlalchemy.MetaData(), first_col,
                                 sqlalchemy.Column(OTHER_COLUMN, sqlalchemy.String(20)))
        table.create(db_engine)
        logger.info('Inserting data into source table %s in %s database ...', src_table.name, database.type)
        row_ids = list(range(1, NO_OF_SRC_ROWS+1))
        if not src_table.use_primary_key:
            # shuffle the first col values for non-incremental mode
            random.shuffle(row_ids)
        for src_row_id in row_ids:  # some databases (like MySQL) will start from 1
            insert_data = {FIRST_COLUMN: src_row_id,
                           OTHER_COLUMN: get_random_string(string.ascii_lowercase, 20)}
            db_engine.execute(table.insert(), [insert_data])

    for target_table in target_tables:
        first_col = sqlalchemy.Column(FIRST_COLUMN, sqlalchemy.Integer, primary_key=target_table.use_primary_key, autoincrement=False)
        logger.info('Creating target table %s in %s database ...', target_table.name, database.type)
        table = sqlalchemy.Table(target_table.name, sqlalchemy.MetaData(), first_col,
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
@pytest.mark.parametrize('number_of_threads', [1, 5])
@pytest.mark.parametrize('per_batch_strategy', ["SWITCH_TABLES", "PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE"])
@pytest.mark.parametrize('partitioning_mode', ["DISABLED", "BEST_EFFORT"])
@pytest.mark.parametrize('non_incremental', [True, False])
@pytest.mark.timeout(300)
@sdc_min_version('2.5.0.0')
def test_jdbc_multitable_consumer_to_jdbc(sdc_builder, sdc_executor, database,
                                          table_name_characters,
                                          table_name_length,
                                          no_of_tables,
                                          number_of_threads,
                                          per_batch_strategy,
                                          partitioning_mode,
                                          non_incremental):
    """Tests Multithreaded Multi-table JDBC source. Replicates a set of tables with prefix 'src' to a another
    set of tables with 'target' prefix. Also leveraging the NO_MORE_DATA EVENT by the Multi-table JDBC source after
    no data in tables. On Event path, the pipeline will execute the pipeline finisher if the event type is seen to
    be no-more-data
    The pipeline would look like:

            jdbc_multitable_consumer >> jdbc_query_dest
                                     >= stream_selector >> finisher
    """

    if non_incremental and Version(sdc_builder.version) < Version('3.0.0.0'):
        # non-incremental support was only added as of SDC 3.0.0.0
        raise pytest.skip('Skipping because SDC builder version {sdc_builder.version} is less than 3.0.0.0')

    event_table_name = get_random_string(string.ascii_lowercase, 10)
    update_event_table_statement = f'UPDATE {event_table_name} set {EVENT_COLUMN_NAME} = 1'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')

    table_configs = [{'tablePattern': f'{SRC_TABLE_PREFIX}%',
                      'partitioningMode': partitioning_mode,
                      'partitionSize': PARTITION_SIZE}]
    if Version(sdc_builder.version) >= Version('3.0.0.0'):
        table_configs[0]['enableNonIncremental'] = non_incremental
    jdbc_multitable_consumer.set_attributes(number_of_threads=number_of_threads,
                                            per_batch_strategy=per_batch_strategy,
                                            maximum_pool_size=number_of_threads,
                                            minimum_idle_connections=number_of_threads,
                                            table_configs=table_configs)

    if partitioning_mode == 'BEST_EFFORT' and Version(sdc_builder.version) < Version('3.0.0.0'):
        # pipeline upgraded across 3.0 boundary with partitioning; default resulting queriesPerSecond will be
        # unacceptably slow for partitioning, so set query interval to 0 instead
        jdbc_multitable_consumer.query_interval = 0

    # The target used to replicate is JDBCQueryExecutor.
    # After SDC-5757 is resolved, we can use JDBCProducer.
    jdbc_query_dest = pipeline_builder.add_stage('JDBC Query', type='executor')
    table_name = (f"${{str:replace(record:attribute('jdbc.tables'),"
                  f"'{SRC_TABLE_PREFIX if not database.type == 'Oracle' else SRC_TABLE_PREFIX.upper()}',"
                  f"'{TGT_TABLE_PREFIX}')}}")
    query = (f"INSERT into {table_name} values "
             f"(${{record:value('/{FIRST_COLUMN if not database.type == 'Oracle' else FIRST_COLUMN.upper()}')}}"
             f", '${{record:value('/{OTHER_COLUMN if not database.type == 'Oracle' else OTHER_COLUMN.upper()}')}}')")

    if Version(sdc_builder.version) < Version('3.14.0'):
        jdbc_query_dest.set_attributes(sql_query=query)
    else:
        jdbc_query_dest.set_attributes(sql_queries=[query])

    finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    jdbc_multitable_consumer >> jdbc_query_dest
    jdbc_multitable_consumer >= finisher

    non_inc = ', non-incremental' if non_incremental else ''
    pipeline_name = (f'JDBC multitable consumer pipeline - {per_batch_strategy} batch strategy, '
                     f'{number_of_threads} threads, {partitioning_mode} partitioning{non_inc}')
    pipeline = pipeline_builder.build(pipeline_name).configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    # Generate random table names.
    table_names = ['{}_{}'.format(get_random_string(table_name_characters, table_name_length).lower(), tableNo)
                   for tableNo in range(0, no_of_tables)]

    random.shuffle(table_names)

    # when using non-incremental mode, give only half the tables primary keys
    pk_tables = table_names[:len(table_names)//2] if non_incremental else table_names

    # build tuples with table name, and whether to use a primary key
    src_tables = [TableInfo(name=TABLE_PREFIX_NAME_FMT.format(table_prefix=SRC_TABLE_PREFIX,
                                                              table_name=table_name),
                            use_primary_key=table_name in pk_tables)
                  for table_name in table_names]
    target_tables = [TableInfo(name=TABLE_PREFIX_NAME_FMT.format(table_prefix=TGT_TABLE_PREFIX,
                                                                 table_name=table_name),
                               use_primary_key=table_name in pk_tables)
                     for table_name in table_names]
    try:
        setup_tables(database, src_tables, target_tables, event_table_name)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert_tables_replicated(database, src_tables)
    finally:
        logger.info('Dropping test related tables in %s database...', database.type)
        teardown_tables(database, [table.name for table in src_tables + target_tables] + [event_table_name])
