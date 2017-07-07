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
import string
from time import sleep
from os.path import dirname, join, realpath

from testframework import environment, sdc, sdc_models
from testframework.markers import *
from testframework.utils import get_random_string

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Validate end-to-end case with stopping pipeline after it read all the data from database. This
# test case uses the query JDBC origin.
@database
def test_query_jdbc_no_more_date(args):
    db = environment.Database(database=args.database,
                              username=args.database_username,
                              password=args.database_password)
    db_cursor = db.connection.cursor()
    pipeline = sdc_models.Pipeline(
        join(dirname(realpath(__file__)),
             'pipelines',
             'query_jdbc_no_more_data.json')
    ).configure_for_environment(db)

    try:
        db_cursor.execute('create table data(col int)')
        db_cursor.execute('insert into data values(1), (2), (3)')

        with sdc.DataCollector(version=args.sdc_version) as data_collector:
            data_collector.add_pipeline(pipeline)
            data_collector.start()

            data_collector.start_pipeline(pipeline).wait_for_finished()

            metrics = data_collector.pipeline_history(pipeline).latest.metrics
            assert metrics.counter("pipeline.batchCount.counter").count == 2
            assert metrics.counter("pipeline.batchInputRecords.counter").count == 3
            assert metrics.counter("pipeline.batchOutputRecords.counter").count == 5

    finally:
        db_cursor.execute('drop table data')


# While writing a simple JDBC multitable consumer => Hive test, we discovered that the origin had
# problems with table names that started with numbers (SDC-5381), so let's use parametrization to
# run the test with various combinations of table name characters and table name lengths.
@cluster
@database
@pytest.mark.parametrize('table_name_characters', [string.ascii_letters, string.digits])
@pytest.mark.parametrize('table_name_length', [8, 20])
def test_jdbc_multitable_consumer_to_hive(args, table_name_characters, table_name_length):
    cluster = environment.Cluster(cluster_server=args.cluster_server)
    db = environment.Database(database=args.database)
    pipeline = sdc_models.Pipeline(
        join(dirname(realpath(__file__)),
             'pipelines',
             'multitable_jdbc_to_hive_replication.json')
    ).configure_for_environment(cluster, db)

    # Generate two random strings to use when naming the DB tables at the source.
    random_table_name_1 = get_random_string(table_name_characters, table_name_length)
    random_table_name_2 = get_random_string(table_name_characters, table_name_length)

    try:
        # Get cursors for the JDBC and Hive databases now so that, if anything goes wrong,
        # we can be sure we can use them to clean up tables in the finally block.
        db_cursor = db.connection.cursor()
        hive_cursor = cluster.hive.client.cursor()

        # Create table and load data in the JDBC database.
        for table_name in (random_table_name_1, random_table_name_2):
            logger.info('Creating table %s in %s database...', table_name, db.type)
            # TODO: Update the CREATE TABLE query to allow this test to run against any JDBC
            # database. At the moment, we're assuming that we're running against MySQL.
            db_cursor.execute('CREATE TABLE `{0}` ('
                              'event_id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,'
                              'order_id INT(11),'
                              'event_time DATETIME,'
                              'event_type VARCHAR(32)'
                              ')'.format(table_name))

            logger.info('Adding three rows into %s database...', db.type)
            rows = [
                (123, '2016-08-21 18:23:45', 'SHIPPED'),
                (234, '2016-08-22 19:34:56', 'ARRIVED'),
                (345, '2016-08-23 20:45:12', 'READY')
            ]
            db_cursor.execute('INSERT INTO `{0}` (order_id, event_time, event_type) '
                              'VALUES {1}'.format(table_name,
                                                  ','.join(str(row) for row in rows)))

        with sdc.DataCollector(version=args.sdc_version) as data_collector:
            data_collector.add_pipeline(pipeline)
            data_collector.start()
            data_collector.start_pipeline(pipeline)

            # TODO: Change the wait to depend on record metrics instead of a dumb sleep.
            logger.info('Waiting 15 seconds for pipeline to work...')
            sleep(15)

            # We need to stop the pipeline to ensure that files are properly created in HDFS.
            data_collector.stop_pipeline(pipeline).wait_for_stopped()

        # Check that the data shows up in Hive.
        for table_name in (random_table_name_1, random_table_name_2):
            hive_cursor.execute('SELECT * from `{0}`'.format(table_name))
            # Before we do the assert, we need to prepend event_ids to each row (the
            # auto-incrementing column from our table).
            event_ids = [1, 2, 3]
            assert hive_cursor.fetchall() == [(id,) + row for id, row in zip(event_ids, rows)]
    finally:
        for table_name in (random_table_name_1, random_table_name_2):
            logger.info('Dropping table %s in %s database...', table_name, db.type)
            db_cursor.execute('DROP TABLE `{0}`'.format(table_name))

            logger.info('Dropping table %s in Hive...', table_name)
            hive_cursor.execute('DROP TABLE `{0}`'.format(table_name))
