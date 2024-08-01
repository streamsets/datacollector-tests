# Copyright 2018 StreamSets Inc.
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
import logging
import os
import string
import time

import pytest
from streamsets.sdk.utils import Version
from streamsets.testframework.environments.cloudera import ClouderaManagerCluster
from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# Specify a port for SDC RPC stages to use.
SDC_RPC_PORT = 20000


@pytest.fixture(autouse=True)
def version_check(sdc_builder, cluster):
    if cluster.version.startswith('cdh6.0') and Version(sdc_builder.version) < Version('3.7.0'):
        pytest.skip('HBase destination is not included in streamsets-datacollector-cdh_6_0-lib until SDC 3.7.0 (SDC-9976)')


@cluster('cdh', 'hdp')
def test_hbase_destination_validate_no_config_issues(sdc_builder, sdc_executor, cluster):
    """Simple HBase destination pipeline validation test.
    dev_raw_data_source >> hbase
    """

    # Generate random records.
    dumb_data = ['I see you driving', 'Round town with the girl I love', 'And I am like John.']

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    # Get pipeline builder.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = '\n'.join(dumb_data)

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/text'
    hbase.fields = [dict(columnValue='/text', columnStorageType='TEXT', columnName='cf1:cq1')]

    # Build pipeline.
    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf1': {}})

        # Validate pipeline by running preview.
        preview = sdc_executor.run_pipeline_preview(pipeline, timeout=120_000).preview
        assert preview is not None
        logger.debug('preview is not None')
        assert preview.issues.issues_count == 0
        logger.debug('issues_count is = 0')

    finally:
        # Delete table.
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)
        logger.info('removing pipeline')


@cluster('cdh', 'hdp')
def test_hbase_destination_invalid_configs(sdc_builder, sdc_executor, cluster):
    """HBase destination test checking invalid static and dynamic wrong configurations (Statics: zookeeper_quorum,
    zookeeper_parent_znode. Dynamics: zookeeper_client_port)
    dev_raw_data_source >> hbase
    """

    # Generate random records.
    dumb_data = ['I see you driving', 'Round town with the girl I love', 'And I am like John.']

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    # Get pipeline builder.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = '\n'.join(dumb_data)

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/text'
    hbase.fields = [dict(columnValue='/text', columnStorageType='TEXT', columnName='cf1:cq1')]
    hbase.zookeeper_quorum = None
    hbase.zookeeper_client_port = 0
    hbase.zookeeper_parent_znode = None
    hbase.time_basis = '${time:now()}'

    # Build pipeline.
    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build()
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf1': {}})

        issues = sdc_executor.api_client.export_pipeline(pipeline.id)['pipelineConfig']['issues']

        assert 0 < issues['issueCount']

        stage_issues = issues['stageIssues']['HBase_01']
        assert 2 == len(stage_issues)

        zookeeper_quorum_issue = stage_issues[0]
        assert 'hBaseConnectionConfig.zookeeperQuorum' in zookeeper_quorum_issue['configName']
        assert 'VALIDATION_0007 - Configuration value is required' in zookeeper_quorum_issue['message']

        zookeeper_parent_znode_issue = stage_issues[1]
        assert 'hBaseConnectionConfig.zookeeperParentZNode' in zookeeper_parent_znode_issue['configName']
        assert 'VALIDATION_0007 - Configuration value is required' in zookeeper_parent_znode_issue['message']

        sdc_executor.remove_pipeline(pipeline)
        pipeline = pipeline_builder.build().configure_for_environment(cluster)
        pipeline.configuration['shouldRetry'] = False
        sdc_executor.add_pipeline(pipeline)

        with pytest.raises(Exception) as e:
            sdc_executor.start_pipeline(pipeline)
            sdc_executor.stop_pipeline(pipeline)

        assert 'HBASE_13' in e.value.message
        assert 'HBASE_13 - Zookeeper client port is invalid' in e.value.message

    finally:
        # Delete table.
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)
        logger.info('removing pipeline')


@cluster('cdh', 'hdp')
def test_hbase_destination_single_record_text_storage(sdc_builder, sdc_executor, cluster):
    """Simple HBase destination test using JSON data.
    dev_raw_data_source >> hbase
    """

    # Generate dummy data.
    data = {'key': 'key', 'cf:a': '20', 'cf:b': '20180702113435000', 'cf:c': '20180702113435000',
            'cf:d': '20180702113435000'}
    json_data = json.dumps(data)

    # Expected output.
    expected_key = b'key'
    expected_data = {b'cf:a': b'20', b'cf:b': b'20180702113435000', b'cf:c': b'20180702113435000',
                     b'cf:d': b'20180702113435000'}

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json_data

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/key'
    hbase.fields = [dict(columnValue='/cf:a', columnStorageType='TEXT', columnName='cf:a'),
                    dict(columnValue='/cf:b', columnStorageType='TEXT', columnName='cf:b'),
                    dict(columnValue='/cf:c', columnStorageType='TEXT', columnName='cf:c'),
                    dict(columnValue='/cf:d', columnStorageType='TEXT', columnName='cf:d')]

    hbase.set_attributes(implicit_field_mapping=False)

    # Build pipeline.
    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.stop_pipeline(pipeline)

        for element in cluster.hbase.client.table(random_table_name).scan():
            assert element[0] == expected_key
            assert element[1] == expected_data

    finally:
        # Delete table.
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_implicit_field_mapping_null(sdc_builder, sdc_executor, cluster):
    """HBase test input field as none, should return empty string
    dev_raw_data_source >> hbase
    """

    data = {'key': 'key', 'cf:a': None, 'cf:b': '20180702113435000', 'cf:c': '20180702113435000',
            'cf:d': '20180702113435000'}
    json_data = json.dumps(data)

    expected_key = b'key'
    expected_data = {b'cf:a': b'', b'cf:b': b'20180702113435000', b'cf:c': b'20180702113435000',
                     b'cf:d': b'20180702113435000'}

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json_data

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/key'
    hbase.fields = [dict(columnValue='/cf:a', columnStorageType='TEXT', columnName='cf:a'),
                    dict(columnValue='/cf:b', columnStorageType='TEXT', columnName='cf:b'),
                    dict(columnValue='/cf:c', columnStorageType='TEXT', columnName='cf:c'),
                    dict(columnValue='/cf:d', columnStorageType='TEXT', columnName='cf:d')]
    hbase.set_attributes(on_record_error='DISCARD',
                         implicit_field_mapping=True,
                         ignore_invalid_column=False)

    # Build pipeline.
    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.stop_pipeline(pipeline)

        for element in cluster.hbase.client.table(random_table_name).scan():
            assert expected_key == element[0]
            assert expected_data == element[1]
    finally:
        # Delete table.
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_field_mapping(sdc_builder, sdc_executor, cluster):
    """HBase only field mapping
    dev_raw_data_source >> hbase
    """

    data = {'cf:a': 'data', 'data1': '20180702113435000', 'data2': '20180702113435000'}
    json_data = json.dumps(data)

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = json_data

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/text'
    hbase.fields = [dict(columnValue='/cf:a', columnStorageType='TEXT', columnName='cf:a')]
    hbase.set_attributes(implicit_field_mapping=True)

    # Build pipeline.
    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        sdc_executor.start_pipeline(pipeline)

        sdc_executor.stop_pipeline(pipeline)

        for element in cluster.hbase.client.table(random_table_name).scan():
            assert json_data in str(element)

    finally:
        # Delete table.

        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_field_mapping_ignore_invalid_column(sdc_builder, sdc_executor, cluster):
    """HBase field mapping ignore invalid column
    dev_raw_data_source >> hbase
    """

    data = {'key': 'key', 'Hello': '20180702113435000', 'cf:b': '20180702113435000'}
    json_data = json.dumps(data)

    expected_key = b'key'
    expected_data = {b'cf:a': b'', b'cf:b': b'20180702113435000'}

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json_data

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/key'
    hbase.fields = [dict(columnValue='/cf:a', columnStorageType='TEXT', columnName='cf:a'),
                    dict(columnValue='/cf:b', columnStorageType='TEXT', columnName='cf:b')]
    hbase.set_attributes(on_record_error='DISCARD',
                         implicit_field_mapping=True,
                         ignore_invalid_column=True)

    # Build pipeline.
    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.stop_pipeline(pipeline)

        for element in cluster.hbase.client.table(random_table_name).scan():
            assert expected_key == element[0]
            assert expected_data == element[1]
    finally:
        # Delete table.
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_field_mapping_not_ignore_invalid_column(sdc_builder, sdc_executor, cluster):
    """HBase field mapping not ignore invalid column
    should discard the record
    dev_raw_data_source >> [hbase, wiretap.destination]
    """

    data = {'key': 'key', 'Hello': '20180702113435000', 'cf:b': '20180702113435000'}
    json_data = json.dumps(data)

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                           raw_data=json_data,
                                                                                           stop_after_first_batch=True)

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.fields = [dict(columnValue='/cf:a', columnStorageType='TEXT', columnName='cf:a'),
                    dict(columnValue='/cf:b', columnStorageType='TEXT', columnName='cf:b')]
    hbase.set_attributes(table_name=random_table_name,
                         row_key='/key',
                         on_record_error='TO_ERROR',
                         implicit_field_mapping=True,
                         ignore_invalid_column=False)

    wiretap = pipeline_builder.add_wiretap()

    # Build pipeline.
    dev_raw_data_source >> [hbase, wiretap.destination]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        scan = cluster.hbase.client.table(random_table_name).scan()

        assert 0 == len(list(scan))

        assert 1 == len(wiretap.error_records)
        assert 'HBASE_28' == wiretap.error_records[0].header['errorCode']
    finally:
        # Delete table.
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_write_wrong_column(sdc_builder, sdc_executor, cluster):
    """HBase write records into a wrong column
    should send the record to error records
    dev_raw_data_source >> [hbase, wiretap.destination]
    """

    data = {'key': 'key', 'invalidcf:a': 'Hi', 'cf:b': 'Hello'}
    json_data = json.dumps(data)

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                           raw_data=json_data,
                                                                                           stop_after_first_batch=True)

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')

    hbase.fields = [dict(columnValue='/cf:a', columnStorageType='TEXT', columnName='cf:a'),
                    dict(columnValue='/cf:b', columnStorageType='TEXT', columnName='cf:b')]

    hbase.set_attributes(table_name=random_table_name,
                         storage_type='TEXT',
                         row_key='/key',
                         on_record_error='TO_ERROR',
                         implicit_field_mapping=True,
                         ignore_invalid_column=False)

    wiretap = pipeline_builder.add_wiretap()

    # Build pipeline.
    dev_raw_data_source >> [hbase, wiretap.destination]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        try:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

            assert 0 == len(list(cluster.hbase.client.table(random_table_name).scan()))

            assert 1 == len(wiretap.error_records)
            assert 'HBASE_10' == wiretap.error_records[0].header['errorCode']
        except Exception as e:
            assert 'HBASE_02' in e.response['message']

    finally:
        # Delete table.
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_invalid_row_key(sdc_builder, sdc_executor, cluster):
    """
    HBase invalid row key

    dev_raw_data_source >> [hbase, wiretap.destination]
    """

    data = {'data1': '20180702113435000', 'data2': '20180702113435000'}
    json_data = json.dumps(data)

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                           raw_data=json_data,
                                                                                           stop_after_first_batch=True)

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.fields = [dict(columnValue='[1]', columnStorageType='TEXT', columnName='cf:a')]
    hbase.set_attributes(table_name=random_table_name,
                         row_key='/invalid',
                         on_record_error='TO_ERROR',
                         implicit_field_mapping=True,
                         ignore_invalid_column=False)

    wiretap = pipeline_builder.add_wiretap()

    # Build pipeline.
    dev_raw_data_source >> [hbase, wiretap.destination]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert 0 == len(list(cluster.hbase.client.table(random_table_name).scan()))

        assert 1 == len(wiretap.error_records)
        assert 'HBASE_27' == wiretap.error_records[0].header['errorCode']

    finally:
        # Delete table.
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_not_flat_map(sdc_builder, sdc_executor, cluster):
    """HBase not flat map record
    dev_raw_data_source >> hbase
    """

    inner_data = {'Martin': 'Head of Spain', 'Danilo': 'Customer Support'}

    data = {'Tucu': 'Developer', 'Xavi': 'Developer', 'Alex': 'Developer', 'Others': inner_data}
    json_data = json.dumps(data)

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = json_data

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/text'
    hbase.fields = [dict(columnValue='[1]', columnStorageType='TEXT', columnName='cf:a')]

    hbase.set_attributes(implicit_field_mapping=True,
                         ignore_invalid_column=False)

    # Build pipeline.
    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.stop_pipeline(pipeline)

        for element in cluster.hbase.client.table(random_table_name).scan():
            assert json_data in str(element)

    finally:
        # Delete table.
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_not_map_error(sdc_builder, sdc_executor, cluster):
    """
    HBase not map record (list)
    dev_raw_data_source >> [hbase, wiretap.destination]
    """

    data = ['key', 'Hi', 'Hello', 'Hola']
    json_data = json.dumps(data)

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                           raw_data=json_data,
                                                                                           stop_after_first_batch=True)

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')

    hbase.fields = [dict(columnValue='[1]', columnStorageType='TEXT', columnName='cf:a'),
                    dict(columnValue='[2]', columnStorageType='TEXT', columnName='cf:b'),
                    dict(columnValue='[3]', columnStorageType='TEXT', columnName='cf:c')]

    hbase.set_attributes(table_name=random_table_name,
                         row_key='/key',
                         on_record_error='TO_ERROR',
                         implicit_field_mapping=False,
                         ignore_invalid_column=True)

    wiretap = pipeline_builder.add_wiretap()

    # Build pipeline.
    dev_raw_data_source >> [hbase, wiretap.destination]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        scan = cluster.hbase.client.table(random_table_name).scan()

        assert 0 == len(list(scan))

        assert 1 == len(wiretap.error_records)
        assert 'HBASE_27' == wiretap.error_records[0].header['errorCode']

    finally:
        # Delete table.
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_destination_single_record_binary_storage(sdc_builder, sdc_executor, cluster):
    """Simple HBase destination test using JSON data and storing binary data in HBase.
    dev_raw_data_source >> hbase
    """

    data = {'row_key': 11, 'cf:a': 20, 'cf:b': 30, 'cf:c': 40, 'cf:d': 50}
    json_data = json.dumps(data)
    expected_key = b'\x00\x00\x00\x0b'
    expected_value = {b'cf:a': b'\x00\x00\x00\x14', b'cf:b': b'\x00\x00\x00\x1e', b'cf:c': b'40', b'cf:d': b'50'}

    # Create random table name to avoid collisions
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json_data

    # Add HBase stage to pipeline
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/row_key'
    hbase.fields = [dict(columnValue='/cf:a', columnStorageType='BINARY', columnName='cf:a'),
                    dict(columnValue='/cf:b', columnStorageType='BINARY', columnName='cf:b'),
                    dict(columnValue='/cf:c', columnStorageType='TEXT', columnName='cf:c'),
                    dict(columnValue='/cf:d', columnStorageType='TEXT', columnName='cf:d')]
    hbase.set_attributes(storage_type='BINARY')

    # Build pipeline.
    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.stop_pipeline(pipeline)

        elements = list(cluster.hbase.client.table(random_table_name).scan())
        assert 1 == len(elements)
        for element in elements:
            assert 2 == len(element)
            assert element[0] == expected_key
            assert element[1] == expected_value

    finally:
        # Delete table.
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_destination_single_record_binary_storage_date(sdc_builder, sdc_executor, cluster):
    """HBase destination test using JSON data checking field type conversion from DATE to Binary fails
    dev_raw_data_source >> expression_evaluator >> field_type_converter >> hbase
    """
    single_record_binary_storage_date_time_types(sdc_builder, sdc_executor, cluster, 'DATE')


@cluster('cdh', 'hdp')
def test_hbase_destination_single_record_binary_storage_time(sdc_builder, sdc_executor, cluster):
    """HBase destination test using JSON data checking field type conversion from TIME to Binary fails
    dev_raw_data_source >> expression_evaluator >> field_type_converter >> hbase
    """
    single_record_binary_storage_date_time_types(sdc_builder, sdc_executor, cluster, 'TIME')


@cluster('cdh', 'hdp')
def test_hbase_destination_single_record_binary_storage_date_time(sdc_builder, sdc_executor, cluster):
    """HBase destination test using JSON data checking field type conversion from DATETIME to Binary fails
    dev_raw_data_source >> expression_evaluator >> field_type_converter >> hbase
    """
    single_record_binary_storage_date_time_types(sdc_builder, sdc_executor, cluster, 'DATETIME')


@cluster('cdh', 'hdp')
def test_hbase_multiple_records(sdc_builder, sdc_executor, cluster):
    """HBase write multiple records
    dev_raw_data_source >> hbase
    """

    data = """{"key": "key", "cf:a": "10", "cf:b": "1992-12-02"}
    {"key": "key", "cf:a": "30", "cf:b": "1992-12-03"}
    {"key": "key", "cf:a": "20", "cf:b": "1992-12-01"}"""

    expected_key = b'key'
    expected_data = {b'cf:a': b'20', b'cf:b': b'1992-12-01'}

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = data

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/key'

    hbase.fields = [dict(columnValue='/cf:a', columnStorageType='TEXT', columnName='cf:a'),
                    dict(columnValue='/cf:b', columnStorageType='TEXT', columnName='cf:b')]

    hbase.set_attributes(storage_type='TEXT',
                         on_record_error='TO_ERROR',
                         implicit_field_mapping=False,
                         ignore_invalid_column=False)

    # Build pipeline.
    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.stop_pipeline(pipeline)

        scan = cluster.hbase.client.table(random_table_name).scan()

        assert 0 != len(list(scan))

        for element in scan:
            assert element[0] == expected_key
            assert element[1] == expected_data

    finally:
        # Delete table.
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_collection_types(sdc_builder, sdc_executor, cluster):
    """HBase different collection types
    dev_raw_data_source >> hbase
    """

    data_array = ['Hi', 'Hello', 'Hola']
    data_map = {'English': 'Hello', 'Spanish': 'Hola'}
    data = {'key': 'key', 'cf:a': 'Text_Sample', 'cf:b': data_map, 'cf:c': data_array}
    json_data = json.dumps(data)

    expected_key = b'key'
    expected_data = {b'cf:a': b'Text_Sample', b'cf:b': b'{"English":"Hello","Spanish":"Hola"}',
                     b'cf:c': b'["Hi","Hello","Hola"]'}

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json_data

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/key'

    hbase.fields = [dict(columnValue='/cf:a', columnStorageType='TEXT', columnName='cf:a'),
                    dict(columnValue='/cf:b', columnStorageType='JSON_STRING', columnName='cf:b'),
                    dict(columnValue='/cf:c', columnStorageType='JSON_STRING', columnName='cf:c')]

    hbase.set_attributes(storage_type='TEXT',
                         on_record_error='DISCARD',
                         implicit_field_mapping=False,
                         ignore_invalid_column=False)

    # Build pipeline.
    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.stop_pipeline(pipeline)

        for element in cluster.hbase.client.table(random_table_name).scan():
            assert expected_key == element[0]
            assert expected_data == element[1]

    finally:
        # Delete table.

        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_write_records_on_error_discard(sdc_builder, sdc_executor, cluster):
    """HBase write records on error discard
    dev_raw_data_source >> hbase
    """

    data = {'key': 'key', 'cf:a': 20, 'cf:b': 'Hello', 'cf:c': 'Hello', 'cf:d': 21}
    json_data = json.dumps(data)

    # Create random table name to avoid collisions
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json_data

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/key'

    hbase.fields = [dict(columnValue='/cf:a', columnStorageType='BINARY', columnName='cf:a'),
                    dict(columnValue='/cf:b', columnStorageType='BINARY', columnName='cf:b'),
                    dict(columnValue='/cf:c', columnStorageType='TEXT', columnName='cf:c'),
                    dict(columnValue='/cf:d', columnStorageType='TEXT', columnName='cf:d')]

    hbase.set_attributes(storage_type='TEXT',
                         on_record_error='DISCARD',
                         implicit_field_mapping=False,
                         ignore_invalid_column=False)

    # Build pipeline.
    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.stop_pipeline(pipeline)

        assert 0 == len(list(cluster.hbase.client.table(random_table_name).scan()))

    finally:
        # Delete table.

        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_write_records_on_error_send_to_error(sdc_builder, sdc_executor, cluster):
    """
    HBase write records on error send to error

    dev_raw_data_source >> [hbase, wiretap.destination]
    """

    data = {'key': 'key', 'cf:a': 20, 'cf:b': 'Hello', 'cf:c': 'Hello', 'cf:d': 21}
    json_data = json.dumps(data)

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                           raw_data=json_data,
                                                                                           stop_after_first_batch=True)

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')

    hbase.fields = [dict(columnValue='/cf:a', columnStorageType='BINARY', columnName='cf:a'),
                    dict(columnValue='/cf:b', columnStorageType='BINARY', columnName='cf:b'),
                    dict(columnValue='/cf:c', columnStorageType='TEXT', columnName='cf:c'),
                    dict(columnValue='/cf:d', columnStorageType='TEXT', columnName='cf:d')]

    hbase.set_attributes(table_name=random_table_name,
                         row_key='/key',
                         storage_type='TEXT',
                         on_record_error='TO_ERROR',
                         implicit_field_mapping=False,
                         ignore_invalid_column=False)

    wiretap = pipeline_builder.add_wiretap()

    # Build pipeline.
    dev_raw_data_source >> [hbase, wiretap.destination]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert 0 == len(list(cluster.hbase.client.table(random_table_name).scan()))

        assert 1 == len(wiretap.error_records)
        assert 'HBASE_12' == wiretap.error_records[0].header['errorCode']
    finally:
        # Delete table.
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_write_records_on_error_stop_pipeline(sdc_builder, sdc_executor, cluster):
    """
    HBase write records on error stop pipeline
    dev_raw_data_source >> hbase
    """

    data = {'key': 'key', 'cf:a': 20, 'cf:b': 'on_error_stop_pipeline', 'cf:c': 'Hello', 'cf:d': 21}
    json_data = json.dumps(data)

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json_data

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/key'

    hbase.fields = [dict(columnValue='/non-existing', columnStorageType='BINARY', columnName='cf:a'),
                    dict(columnValue='/cf:b', columnStorageType='BINARY', columnName='cf:b'),
                    dict(columnValue='/cf:c', columnStorageType='TEXT', columnName='cf:c'),
                    dict(columnValue='/cf:d', columnStorageType='TEXT', columnName='cf:d')]

    hbase.set_attributes(storage_type='BINARY',
                         on_record_error='STOP_PIPELINE',
                         implicit_field_mapping=False,
                         ignore_invalid_column=False)

    # Build pipeline.
    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        with pytest.raises(Exception):
            sdc_executor.start_pipeline(pipeline)
            sdc_executor.stop_pipeline(pipeline)

        status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        logger.info('Pipeline status %s ...', status)
        assert 'RUN_ERROR' == status

    finally:
        # Delete table.

        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_multiple_records_on_error(sdc_builder, sdc_executor, cluster):
    """
    HBase write multiple records
    dev_raw_data_source >> [hbase, wiretap.destination]
    """

    data = """{"key": "key", "cf:a": "10", "cf:b": "1992-12-02"}
    {"key": "key", "cf:a": "30", "cf:b": "1992-12-03"}
    {"key": "key", "cf:a": "20", "cf:b": "1992-12-01"}"""

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                           raw_data=data,
                                                                                           stop_after_first_batch=True)

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')

    hbase.fields = [dict(columnValue='/cf:a', columnStorageType='TEXT', columnName='cf:a'),
                    dict(columnValue='/cf:b', columnStorageType='BINARY', columnName='cf:b')]

    hbase.set_attributes(table_name=random_table_name,
                         row_key='/key',
                         storage_type='TEXT',
                         on_record_error='TO_ERROR',
                         implicit_field_mapping=False,
                         ignore_invalid_column=False)

    wiretap = pipeline_builder.add_wiretap()

    # Build pipeline.
    dev_raw_data_source >> [hbase, wiretap.destination]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        scan = cluster.hbase.client.table(random_table_name).scan()

        assert 0 == len(list(scan))

        assert 3 == len(wiretap.error_records)
        assert 'HBASE_12' == wiretap.error_records[0].header['errorCode']
        assert 'HBASE_12' == wiretap.error_records[1].header['errorCode']
        assert 'HBASE_12' == wiretap.error_records[2].header['errorCode']

    finally:
        # Delete table.
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_custom_time_basis(sdc_builder, sdc_executor, cluster):
    """HBase write records with custom time basis
    dev_raw_data_source >> hbase
    """

    data = """{"key": "key", "cf:a": "10", "cf:b": "1992-12-02"}
    {"key": "key", "cf:a": "30", "cf:b": "1992-12-03"}
    {"key": "key", "cf:a": "20", "cf:b": "1992-12-01"}"""

    expected_key = b'key'
    expected_data = {b'cf:a': b'30', b'cf:b': b'Thu Dec 03 00:00:00 GMT 1992'}

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = data

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/key'

    hbase.fields = [dict(columnValue='/cf:a', columnStorageType='TEXT', columnName='cf:a'),
                    dict(columnValue='/cf:b', columnStorageType='TEXT', columnName='cf:b')]

    hbase.set_attributes(storage_type='TEXT',
                         on_record_error='TO_ERROR',
                         implicit_field_mapping=False,
                         ignore_invalid_column=False,
                         time_basis='${record:value(\'/cf:b\')}')

    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter_configs = [
        {
            'fields': ['/cf:b'],
            'targetType': 'DATE',
            'dateFormat': 'YYYY_MM_DD'
        }
    ]
    field_type_converter.set_attributes(conversion_method='BY_FIELD',
                                        field_type_converter_configs=field_type_converter_configs)

    # Build pipeline.
    dev_raw_data_source >> field_type_converter >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.stop_pipeline(pipeline)

        scan = cluster.hbase.client.table(random_table_name).scan()

        assert 0 != len(list(scan))

        for element in scan:
            assert element[0] == expected_key
            assert element[1] == expected_data

    finally:
        # Delete table.

        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_empty_time_basis(sdc_builder, sdc_executor, cluster):
    """HBase write records with empty time basis
    dev_raw_data_source >> hbase
    """

    data = """{"key": "key", "cf:a": "10", "cf:b": "1992-12-02"}
    {"key": "key", "cf:a": "30", "cf:b": "1992-12-03"}
    {"key": "key", "cf:a": "20", "cf:b": "1992-12-01"}"""

    expected_key = b'key'
    expected_data = {b'cf:a': b'20', b'cf:b': b'Tue Dec 01 00:00:00 GMT 1992'}

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = data

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/key'

    hbase.fields = [dict(columnValue='/cf:a', columnStorageType='TEXT', columnName='cf:a'),
                    dict(columnValue='/cf:b', columnStorageType='TEXT', columnName='cf:b')]

    hbase.set_attributes(storage_type='TEXT',
                         on_record_error='TO_ERROR',
                         implicit_field_mapping=False,
                         ignore_invalid_column=False,
                         time_basis='')

    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter_configs = [
        {
            'fields': ['/cf:b'],
            'targetType': 'DATE',
            'dateFormat': 'YYYY_MM_DD'
        }
    ]
    field_type_converter.set_attributes(conversion_method='BY_FIELD',
                                        field_type_converter_configs=field_type_converter_configs)

    # Build pipeline.
    dev_raw_data_source >> field_type_converter >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.stop_pipeline(pipeline)

        scan = cluster.hbase.client.table(random_table_name).scan()

        assert 0 != len(list(scan))

        for element in scan:
            assert element[0] == expected_key
            assert element[1] == expected_data

    finally:
        # Delete table.

        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_now_time_basis(sdc_builder, sdc_executor, cluster):
    """HBase write records with now time basis
    dev_raw_data_source >> hbase
    """

    data = """{"key": "key", "cf:a": "10", "cf:b": "1992-12-02"}
    {"key": "key", "cf:a": "30", "cf:b": "1992-12-03"}
    {"key": "key", "cf:a": "20", "cf:b": "1992-12-01"}"""

    expected_key = b'key'
    expected_data = {b'cf:a': b'20', b'cf:b': b'Tue Dec 01 00:00:00 GMT 1992'}

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = data

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/key'

    hbase.fields = [dict(columnValue='/cf:a', columnStorageType='TEXT', columnName='cf:a'),
                    dict(columnValue='/cf:b', columnStorageType='TEXT', columnName='cf:b')]

    hbase.set_attributes(storage_type='TEXT',
                         on_record_error='TO_ERROR',
                         implicit_field_mapping=False,
                         ignore_invalid_column=False,
                         time_basis='${time:now()}')

    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter_configs = [
        {
            'fields': ['/cf:b'],
            'targetType': 'DATE',
            'dateFormat': 'YYYY_MM_DD'
        }
    ]
    field_type_converter.set_attributes(conversion_method='BY_FIELD',
                                        field_type_converter_configs=field_type_converter_configs)

    # Build pipeline.
    dev_raw_data_source >> field_type_converter >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.stop_pipeline(pipeline)

        scan = cluster.hbase.client.table(random_table_name).scan()

        assert 0 != len(list(scan))

        for element in scan:
            assert element[0] == expected_key
            assert element[1] == expected_data

    finally:
        # Delete table.

        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_destination_invalid_column_family(sdc_builder, sdc_executor, cluster):
    """HBase destination test checking an invalid columnName in hbase destination fields raises an HBASE_32 error
    dev_raw_data_source >> hbase
    """
    # Generate random records.
    dumb_data = ['key', 'invalid_cf_a_value', 'valid_cf_b_value']

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    # Get pipeline builder.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = '\n'.join(dumb_data)

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '[0]'
    hbase.fields = [dict(columnValue='[1]', columnStorageType='BINARY', columnName='invalid_cf:a'),
                    dict(columnValue='[2]', columnStorageType='BINARY', columnName='cf:b')]
    hbase.set_attributes(storage_type='TEXT', implicit_field_mapping=False, ignore_missing_field=False)

    # Build pipeline.
    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf': {}})

        with pytest.raises(Exception) as e:
            sdc_executor.start_pipeline(pipeline)
            sdc_executor.stop_pipeline(pipeline)

        assert 'HBASE_32' in e.value.message
        assert 'HBASE_32 - Column family \'invalid_cf:a\' doesn\'t exist' in e.value.message

    finally:
        # Delete table
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)
        logger.info('removing pipeline')


@cluster('cdh', 'hdp')
def test_hbase_regular_user(sdc_builder, sdc_executor, cluster):
    """HBase destination with regular user
    dev_raw_data_source >> hbase
    """

    data = {'key': 'key', 'cf:a': '20', 'cf:b': '20180702113435000'}
    json_data = json.dumps(data)

    expected_key = b'key'
    expected_data = {b'cf:a': b'20', b'cf:b': b'20180702113435000'}

    # Create random table name to avoid collisions
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json_data

    # Add HBase stage to pipeline
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/key'
    hbase.fields = [dict(columnValue='/cf:a', columnStorageType='TEXT', columnName='cf:a'),
                    dict(columnValue='/cf:b', columnStorageType='TEXT', columnName='cf:b')]

    hbase.set_attributes(storage_type='TEXT',
                         on_record_error='DISCARD',
                         hbase_user='',
                         implicit_field_mapping=False,
                         ignore_invalid_column=False)

    # Build pipeline.
    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.stop_pipeline(pipeline)

        for element in cluster.hbase.client.table(random_table_name).scan():
            assert element[0] == expected_key
            assert element[1] == expected_data

    finally:
        # Delete table.

        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_proxy_user(sdc_builder, sdc_executor, cluster):
    """HBase destination with proxy user
    dev_raw_data_source >> hbase
    """

    # TEST-551: Permissions missing for stage.test_hbase_destination.test_hbase_proxy_user in Kerberised cluster
    if isinstance(cluster, ClouderaManagerCluster) and cluster.hbase.is_kerberized:
        pytest.skip('HBase Proxy User requires additional configuration on the cluster.')

    data = {'key': 'key', 'cf:a': '20', 'cf:b': '20180702113435000'}
    json_data = json.dumps(data)

    expected_key = b'key'
    expected_data = {b'cf:a': b'20', b'cf:b': b'20180702113435000'}

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json_data

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/key'
    hbase.fields = [dict(columnValue='/cf:a', columnStorageType='TEXT', columnName='cf:a'),
                    dict(columnValue='/cf:b', columnStorageType='TEXT', columnName='cf:b')]

    hbase.set_attributes(storage_type='TEXT',
                         on_record_error='DISCARD',
                         hbase_user='foo',
                         implicit_field_mapping=False,
                         ignore_invalid_column=False)

    # Build pipeline.
    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.stop_pipeline(pipeline)

        for element in cluster.hbase.client.table(random_table_name).scan():
            assert element[0] == expected_key
            assert element[1] == expected_data

    finally:
        # Delete table.
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


def single_record_binary_storage_date_time_types(sdc_builder, sdc_executor, cluster, data_type):
    """HBase destination test using JSON data checking fields type conversion
    dev_raw_data_source >> expression_evaluator >> field_type_converter >> [hbase, wiretap.destination]
    """

    data = {'key': 11}
    logger.debug('data_type = %s', data_type)
    data['origin_date'] = time.time()

    json_data = json.dumps(data)

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                           raw_data=json_data,
                                                                                           stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    logger.debug(dir(expression_evaluator.field_expressions))
    expression_evaluator.field_expressions = [
        dict(fieldToSet='/converted_date',
             expression="${time:millisecondsToDateTime(record:value('/origin_date'))}")]

    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter_configs = [
        {
            'fields': ['/converted_date'],
            'targetType': str(data_type),
            'dateFormat': 'YYYY_MM_DD_HH_MM_SS'
        }
    ]
    field_type_converter.set_attributes(conversion_method='BY_FIELD',
                                        field_type_converter_configs=field_type_converter_configs)

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/key'
    hbase.fields = [dict(columnValue='/converted_date', columnStorageType='BINARY', columnName='cf:a')]
    hbase.set_attributes(row_key='/key',
                         table_name=random_table_name,
                         storage_type='BINARY',
                         implicit_field_mapping=False,
                         ignore_missing_field=False)
    wiretap = pipeline_builder.add_wiretap()

    # Build pipeline.
    dev_raw_data_source >> expression_evaluator >> field_type_converter >> [hbase, wiretap.destination]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table.
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf:': {}})

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert 1 == len(wiretap.error_records)

        assert 'HBASE_12' == wiretap.error_records[0].header['errorCode']
        assert 'HBASE_12 - Cannot convert type: ' + str(data_type) + ' to BINARY' == \
               wiretap.error_records[0].header['errorMessage']

        elements = list(cluster.hbase.client.table(random_table_name).scan())
        assert 0 == len(elements)
    finally:
        # Delete table.
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh', 'hdp')
@sdc_min_version('4.1.0')
@pytest.mark.parametrize('column_name', ['name', 'name:f', 'name:f:first'])
def test_hbase_delimiter_column_name(sdc_builder, sdc_executor, cluster, column_name):
    """HBase destination test multiple number of delimiters in column names.
    Pipeline: haddop_fs >> hbase
    """
    # Convert to some raw data for the Dev Raw Data Source.
    raw_data = json.dumps({'name': 'john'})

    # Create random table name to avoid collisions.
    random_table_name = get_random_string(string.ascii_letters, 10)

    # Pipeline:
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Create Dev Raw Data Source stage.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)

    # Add HBase stage to pipeline.
    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/name'
    hbase.fields = [dict(columnValue='/name',
                         columnStorageType='TEXT',
                         columnName=column_name)]

    # Build pipeline.
    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'name:f': {}})

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        if column_name == 'name':
            pytest.fail('Should not reach here')

        # Validate data from HBASE:
        for element in cluster.hbase.client.table(random_table_name).scan():
            assert element[1][column_name.encode('utf-8')] == b'john'
    except Exception as err:
        if column_name != 'name':
            pytest.fail('Should not reach here')
        assert column_name == 'name'
        assert 'HBASE_28' in err.message
    finally:
        # Delete table.
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)
