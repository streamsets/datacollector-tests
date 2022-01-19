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

from streamsets.testframework.markers import cluster
from streamsets.testframework.utils import get_random_string

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


@cluster('cdh')
def test_hive_query_executor(sdc_builder, sdc_executor, cluster):
    """
    Test Hive query executor stage. The pipeline would look like:
    dev_raw_data_source >> hive_query >= wiretap
    """
    hive_table_name = get_random_string(string.ascii_letters, 10)
    hive_cursor = cluster.hive.client.cursor()
    sql_queries = ["CREATE TABLE ${time:now()}"]

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

