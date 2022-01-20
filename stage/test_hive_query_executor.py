# Copyright 2020 StreamSets Inc.
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
import string
import pytest
import time
import pytz

from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


@cluster('cdh')
def test_hive_query_executor(sdc_builder, sdc_executor, cluster):
    """
    Test Hive query executor stage. The pipeline would look like:
    dev_raw_data_source >> hive_query >= wiretap
    """
    hive_table_name = get_random_string(string.ascii_letters, 10)
    hive_cursor = cluster.hive.client.cursor()
    sql_queries = ["CREATE TABLE ${record:value('/text')} (id int, name string)"]

    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=hive_table_name,
                                                                                  stop_after_first_batch=True)
    wiretap = builder.add_wiretap()
    hive_query = builder.add_stage('Hive Query', type='executor').set_attributes(sql_queries=sql_queries)

    dev_raw_data_source >> hive_query >= wiretap.destination

    pipeline = builder.build(title='Hive query executor pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        # assert successful query execution of the pipeline
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert (wiretap.output_records[0].header['values']['sdc.event.type'] == 'successful-query')

        # assert Hive table creation
        assert hive_cursor.table_exists(hive_table_name)

        wiretap.reset()

        # Re-running the same query to create Hive table should fail the query. So assert the failure.
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert wiretap.output_records[0].header['values']['sdc.event.type'] == 'failed-query'
    finally:
        # drop the Hive table
        hive_cursor.execute(f'DROP TABLE `{hive_table_name}`')


@sdc_min_version('4.4.0')
@cluster('cdh')
def test_hive_query_executor_time_el(sdc_builder, sdc_executor, cluster):
    """
    Test Hive query executor stage with TimeEL. The pipeline would look like:
    dev_raw_data_source >> hive_query >= wiretap
    """
    start_test_seconds = round(datetime.now().timestamp())

    hive_table_name = get_random_string(string.ascii_letters, 10)
    hive_cursor = cluster.hive.client.cursor()
    hive_cursor.execute(f'CREATE TABLE `{hive_table_name}` (id int, name string) PARTITIONED BY (day String)')
    sql_queries = [f'ALTER TABLE `{hive_table_name}` ADD PARTITION (day='+"'${time:dateTimeToMilliseconds(time:now())}')"]

    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=hive_table_name,
                                                                                  stop_after_first_batch=True)
    wiretap = builder.add_wiretap()
    hive_query = builder.add_stage('Hive Query', type='executor').set_attributes(sql_queries=sql_queries)

    dev_raw_data_source >> hive_query >= wiretap.destination

    pipeline = builder.build(title='Hive query executor pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        # assert successful query execution of the pipeline
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert (wiretap.output_records[0].header['values']['sdc.event.type'] == 'successful-query')
        wiretap.reset()

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert (wiretap.output_records[0].header['values']['sdc.event.type'] == 'successful-query')
        wiretap.reset()

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert (wiretap.output_records[0].header['values']['sdc.event.type'] == 'successful-query')

        # assert Hive table creation
        assert hive_cursor.table_exists(hive_table_name)

        hive_cursor.execute(f'SHOW PARTITIONS `{hive_table_name}`')
        hive_values = [list(row) for row in hive_cursor.fetchall()]

        end_test_seconds = round(datetime.now().timestamp())
        partition_values = [start_test_seconds]

        for h in hive_values:
            h = int(round(float(h[0].replace("day=", "")) / 1000.0))
            partition_values.append(h)

        partition_values.append(end_test_seconds)
        # assert timestamps are ordered correctly
        assert all(partition_values[i] <= partition_values[i + 1] for i in range(len(partition_values)-1))

    finally:
        # drop the Hive table
        hive_cursor.execute(f'DROP TABLE `{hive_table_name}`')


@sdc_min_version('4.4.0')
@cluster('cdh')
@pytest.mark.parametrize('time_zone', ['UTC', 'America/Los_Angeles', 'America/New_York'])
def test_time_el_with_timezone(sdc_builder, sdc_executor, cluster, time_zone):
    """
    Test Hive query executor stage with TimeEL and timezones. The pipeline would look like:
    dev_raw_data_source >> hive_query >= wiretap
    """
    start_test_seconds = round(datetime.now().timestamp())

    hive_table_name = get_random_string(string.ascii_letters, 10)
    hive_cursor = cluster.hive.client.cursor()
    hive_cursor.execute(f'CREATE TABLE `{hive_table_name}` (id int, name string) PARTITIONED BY (day String)')
    sql_queries = [f'ALTER TABLE `{hive_table_name}` ADD PARTITION (day='+"'${MM()}-${DD()}-${YYYY()} ${hh()}-${mm()}-${ss()}')"]

    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=hive_table_name,
                                                                                  stop_after_first_batch=True)
    wiretap = builder.add_wiretap()
    hive_query = builder.add_stage('Hive Query', type='executor').set_attributes(sql_queries=sql_queries, data_time_zone=time_zone)

    dev_raw_data_source >> hive_query >= wiretap.destination

    pipeline = builder.build(title='Hive query executor pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        # assert successful query execution of the pipeline
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert (wiretap.output_records[0].header['values']['sdc.event.type'] == 'successful-query')
        wiretap.reset()

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert (wiretap.output_records[0].header['values']['sdc.event.type'] == 'successful-query')
        wiretap.reset()

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert (wiretap.output_records[0].header['values']['sdc.event.type'] == 'successful-query')

        # assert Hive table creation
        assert hive_cursor.table_exists(hive_table_name)

        hive_cursor.execute(f'SHOW PARTITIONS `{hive_table_name}`')
        hive_values = [list(row) for row in hive_cursor.fetchall()]

        end_test_seconds = round(datetime.now().timestamp())
        partition_values = [start_test_seconds]

        for h in hive_values:
            h = h[0].replace("day=", "")

            current_date = datetime.strptime(h, "%m-%d-%Y %H-%M-%S")
            current_date = pytz.timezone(time_zone).localize(current_date)

            partition_values.append(round(current_date.timestamp()))

        partition_values.append(end_test_seconds)
        # assert timestamps are ordered correctly
        assert all(partition_values[i] <= partition_values[i + 1] for i in range(len(partition_values)-1))

    finally:
        # drop the Hive table
        hive_cursor.execute(f'DROP TABLE `{hive_table_name}`')
