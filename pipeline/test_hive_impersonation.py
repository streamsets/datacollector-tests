# Copyright 2019 StreamSets Inc.
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

import json
import string
import logging
from datetime import datetime

import pytest
from streamsets.testframework.environments.hortonworks import AmbariCluster
from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.sdc_properties['stage.conf_com.streamsets.pipeline.stage.hive.impersonate.current.user'] = 'true'

    return hook


@pytest.fixture(autouse=True)
def impersonation_check(sdc_executor):
    if sdc_executor.sdc_configuration.get('stage.conf_com.streamsets.pipeline.stage.hive.'
                                          'impersonate.current.user') != 'true':
        pytest.skip('Hive impersonation requires stage.conf_com.streamsets.pipeline.stage.hive.'
                    'impersonate.current.user to be set to true')


@pytest.fixture(autouse=True)
def hive_check(cluster, sdc_builder):
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
        and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')


@cluster('cdh', 'hdp')
def test_hive_query_executor_impersonation(sdc_builder, sdc_executor, cluster):
    """Test Hive query executor stage for current user impersonation.
    This is acheived by using a deduplicator which assures us that there is
    only one successful ingest. The pipeline would look like:

        dev_raw_data_source >> record_deduplicator >> hive_query
                                                   >> trash
    """
    hive_table_name = get_random_string(string.ascii_letters, 10)
    hive_cursor = cluster.hive.client.cursor()
    sql_queries = ["CREATE EXTERNAL TABLE ${record:value('/text')} (id int, name string) LOCATION '/tmp/${record:value('/text')}'"]

    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=hive_table_name)
    record_deduplicator = builder.add_stage('Record Deduplicator')
    trash = builder.add_stage('Trash')

    hive_query = builder.add_stage('Hive Query', type='executor').set_attributes(sql_queries=sql_queries)

    dev_raw_data_source >> record_deduplicator >> hive_query
    record_deduplicator >> trash

    pipeline = builder.build(title='Hive query executor pipeline').configure_for_environment(cluster)
    # HDP Clusterdock inserts Hive username to the JDBC URL which is specifically incompatible with this test
    # as it's validating user impersonation (specifying username in URL is incompatible with forced impersonation).
    hive_query.jdbc_url = hive_query.jdbc_url.replace('hive.server2.proxy.user=hive', '')
    sdc_executor.add_pipeline(pipeline)

    try:
        # assert successful query execution of the pipeline
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)
        hive_cursor.execute(f'DESCRIBE FORMATTED {hive_table_name}')
        table_owner = [row[1].strip() for row in hive_cursor.fetchall() if row[0].strip()=='Owner:']
        assert (snapshot[hive_query.instance_name].event_records[0].header['values']['sdc.event.type'] ==
                'successful-query')

        # assert Hive table creation
        assert hive_cursor.table_exists(hive_table_name)

        # assert table owner is current user
        assert table_owner[0] == 'admin'
    finally:
        # drop the Hive table
        cluster.hdfs.client.delete(f'/tmp/{hive_table_name}', recursive=True)
        hive_cursor.execute(f'DROP TABLE `{hive_table_name}`')


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
@pytest.mark.parametrize('db', ['', 'default', 'custom'])
@pytest.mark.parametrize('stored_as_avro', [True, False])
# TEST-979: HDP Hive is misconfigured
@pytest.mark.parametrize('external_table', [True])
@pytest.mark.parametrize('partitioned', [True, False])
def test_cold_start_impersonation(sdc_builder, sdc_executor, cluster, db, stored_as_avro, external_table, partitioned):
    """Validate Cold Start no table and no data with hive current user impersonation.
       This test also tests different types of table and methods of creation.
       The pipeline looks like:

        dev_raw_data_source >> expression_evaluator >> hive_metadata
        hive_metadata >> hadoop_fs
        hive_metadata >> hive_metastore
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    db_for_path = 'default' if not db else f'{db}.db' if db != 'default' else db
    database_location_for_table_path = (f'/tmp/sdc/hive/warehouse/{db_for_path}'
                                        if external_table else f'/user/hive/warehouse/{db_for_path}')

    table_path_template = f'{database_location_for_table_path}/{table_name}' if external_table else ''

    raw_data = [dict(id=1, name='abc'), dict(id=2, name='def'), dict(id=3, name='ghi')]

    dev_raw_data_source_data = ''.join(json.dumps(d) for d in raw_data)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[{'attributeToSet': 'db',
                                                                       'headerAttributeExpression': db},
                                                                      {'attributeToSet': 'table_name',
                                                                       'headerAttributeExpression': table_name}])

    partition_configuration = [{'name': 'dt', 'valueType': 'STRING',
                                'valueEL': '${YYYY()}-${MM()}-${DD()}'}] if partitioned else []
    partition_path_template = 'dt=${YYYY()}-${MM()}-${DD()}' if partitioned else ''

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO',
                                 database_expression="${record:attribute('db')}",
                                 external_table=external_table,
                                 table_path_template=table_path_template,
                                 partition_configuration=partition_configuration,
                                 partition_path_template=partition_path_template,
                                 decimal_scale_expression='5',
                                 decimal_precision_expression='10',
                                 table_name="${record:attribute('table_name')}")

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(avro_schema_location='HEADER',
                             data_format='AVRO',
                             directory_in_header=True,
                             use_roll_attribute=True)

    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')
    hive_metastore.set_attributes(stored_as_avro=stored_as_avro)

    dev_raw_data_source >> expression_evaluator >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore

    pipeline = pipeline_builder.build(title='Hive drift test - Cold Start').configure_for_environment(cluster)
    # HDP Clusterdock inserts Hive username to the JDBC URL which is specifically incompatible with this test
    # as it's validating user impersonation (specifying username in URL is incompatible with forced impersonation).
    hive_metadata.jdbc_url = hive_metadata.jdbc_url.replace('hive.server2.proxy.user=hive', '')
    hive_metastore.jdbc_url = hive_metastore.jdbc_url.replace('hive.server2.proxy.user=hive', '')
    sdc_executor.add_pipeline(pipeline)

    hive_cursor = cluster.hive.client.cursor()
    if db:
        hive_cursor.execute(f'CREATE DATABASE IF NOT EXISTS`{db}`')
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        hive_cursor.execute('RELOAD {0}'.format(_get_qualified_table_name(db, table_name)))
        hive_cursor.execute('SELECT * from {0}'.format(_get_qualified_table_name(db, table_name)))
        hive_values = [list(row) for row in hive_cursor.fetchall()]
        raw_values = [list(row.values()) for row in raw_data]
        if partitioned:
            for i in range(len(raw_values)):
                raw_values[i] = raw_values[i] + [datetime.now().strftime('%Y-%m-%d')]
        assert sorted(hive_values) == sorted(raw_values)

        hive_cursor.execute('DESCRIBE FORMATTED {0}'.format(_get_qualified_table_name(db, table_name)))
        table_owner = [row[1].strip() for row in hive_cursor.fetchall() if row[0].strip() == 'Owner:']
        assert table_owner[0] == 'admin'

    finally:
        logger.info('Dropping table %s in Hive...', _get_qualified_table_name(db, table_name))
        if external_table:
            logger.info('Deleting Hadoop FS directory %s ...', database_location_for_table_path)
            cluster.hdfs.client.delete(database_location_for_table_path, recursive=True)
        hive_cursor.execute('DROP TABLE {0}'.format(_get_qualified_table_name(db, table_name)))
        if db and db != 'default':
            logger.info('Dropping Database %s in Hive...', db)
            hive_cursor.execute('DROP DATABASE IF EXISTS`{0}`'.format(db))


def _get_qualified_table_name(db, table_name):
    return f'`{db}`.`{table_name}`' if db else f'`{table_name}`'
