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

import json
import logging
import string
import time
from datetime import datetime

import pytest
import sqlalchemy
from streamsets.testframework.markers import cluster, parcelpackaging, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

from . import test_apache

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Specify a port for SDC RPC stages to use.
SDC_RPC_PORT = 20000
SNAPSHOT_TIMEOUT_SEC = 120

DEFAULT_IMPALA_DB = 'default'
DEFAULT_KUDU_PORT = 7051

pytestmark = [parcelpackaging]


@cluster('cdh')
def test_kudu_destination(sdc_builder, sdc_executor, cluster):
    """Simple Dev Raw Data Source to Kudu pipeline.

    dev_raw_data_source >> kudu
    """
    if not hasattr(cluster, 'kudu'):
        pytest.skip('Kudu tests only run against clusters with the Kudu service present.')

    # Generate some data.
    tour_de_france_contenders = [dict(favorite_rank=1, name='Chris Froome', wins=3),
                                 dict(favorite_rank=2, name='Greg LeMond', wins=3),
                                 dict(favorite_rank=4, name='Vincenzo Nibali', wins=1),
                                 dict(favorite_rank=3, name='Nairo Quintana', wins=0)]
    raw_data = ''.join([json.dumps(contender) for contender in tour_de_france_contenders])

    # For a little more coverage, we'll map the "favorite_rank" record field to the "rank" column in Kudu.
    # These rankings are Dima's opinion and not reflective of the views of StreamSets, Inc.
    field_to_column_mapping = [dict(field='/favorite_rank', columnName='rank')]

    kudu_table_name = get_random_string(string.ascii_letters, 10)
    kudu_master_address = '{}:{}'.format(cluster.server_host, DEFAULT_KUDU_PORT)

    # Build the pipeline.
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data)
    kudu = builder.add_stage('Kudu',
                             type='destination').set_attributes(table_name=kudu_table_name,
                                                                default_operation='INSERT',
                                                                field_to_column_mapping=field_to_column_mapping)
    dev_raw_data_source >> kudu

    pipeline = builder.build().configure_for_environment(cluster)
    pipeline.delivery_guarantee = 'AT_MOST_ONCE'
    # We want to write data once and then stop, but Dev Raw Data Source will keep looping, so we set the rate limit to
    # a low value and will rely upon pipeline metrics to know when to stop the pipeline.
    pipeline.rate_limit = 4

    metadata = sqlalchemy.MetaData()
    tdf_contenders_table = sqlalchemy.Table(kudu_table_name,
                                            metadata,
                                            sqlalchemy.Column('rank', sqlalchemy.Integer, primary_key=True),
                                            sqlalchemy.Column('name', sqlalchemy.String),
                                            sqlalchemy.Column('wins', sqlalchemy.Integer),
                                            impala_partition_by='HASH PARTITIONS 16',
                                            impala_stored_as='KUDU',
                                            impala_table_properties={
                                                'kudu.table_name': kudu_table_name,
                                                'kudu.master_addresses': kudu_master_address,
                                                'kudu.num_tablet_replicas': '1'
                                            })

    try:
        logger.info('Creating Kudu table %s ...', kudu_table_name)
        engine = cluster.kudu.engine
        tdf_contenders_table.create(engine)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(len(tour_de_france_contenders))
        sdc_executor.stop_pipeline(pipeline)

        connection = engine.connect()
        result = connection.execute(sqlalchemy.sql.select([tdf_contenders_table]).order_by('rank'))
        assert list(result) == [tuple([item['favorite_rank'], item['name'], item['wins']])
                                for item in sorted(tour_de_france_contenders, key=lambda key: key['favorite_rank'])]
    finally:
        logger.info('Dropping Kudu table %s ...', kudu_table_name)
        tdf_contenders_table.drop(engine)


@cluster('cdh')
def test_kudu_destination_unixtime_micro_datatype(sdc_builder, sdc_executor, cluster):
    """
    Test Kudu's UNIXTIME_MICRO data type support.
    dev_raw_data_source >> kudu
    """
    if not hasattr(cluster, 'kudu'):
        pytest.skip('Kudu tests only run against clusters with the Kudu service present.')

    # Generate some data. Kudu does not store microsecond so set it 0.
    now = datetime.now().replace(microsecond=0)
    now_millisecond = time.mktime(now.timetuple()) * 1000
    input_data = [dict(id=1, time=now_millisecond)]

    raw_data = ''.join([json.dumps(contender) for contender in input_data])
    field_to_column_mapping = [dict(field='/id', columnName='id'),
                               dict(field='/time', columnName='unixtime_micro')]

    kudu_table_name = get_random_string(string.ascii_letters, 10)
    kudu_master_address = f'{cluster.server_host}:{DEFAULT_KUDU_PORT}'

    # Build the pipeline.
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data)
    kudu = builder.add_stage('Kudu',
                             type='destination').set_attributes(table_name=kudu_table_name,
                                                                default_operation='INSERT',
                                                                field_to_column_mapping=field_to_column_mapping)
    dev_raw_data_source >> kudu

    pipeline = builder.build().configure_for_environment(cluster)
    pipeline.delivery_guarantee = 'AT_MOST_ONCE'
    # We want to write data once and then stop, but Dev Raw Data Source will keep looping, so we set the rate limit to
    # a low value and will rely upon pipeline metrics to know when to stop the pipeline.
    pipeline.rate_limit = 4

    metadata = sqlalchemy.MetaData()
    test_table = sqlalchemy.Table(kudu_table_name,
                                  metadata,
                                  sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                                  sqlalchemy.Column('unixtime_micro', sqlalchemy.TIMESTAMP),
                                  impala_partition_by='HASH PARTITIONS 16',
                                  impala_stored_as='KUDU',
                                  impala_table_properties={
                                      'kudu.table_name': kudu_table_name,
                                      'kudu.master_addresses': kudu_master_address,
                                      'kudu.num_tablet_replicas': '1'
                                  })

    try:
        logger.info('Creating Kudu table %s ...', kudu_table_name)
        engine = cluster.kudu.engine
        test_table.create(engine)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(len(input_data))
        sdc_executor.stop_pipeline(pipeline)

        connection = engine.connect()
        result = connection.execute(sqlalchemy.sql.select([test_table])).fetchone()
        assert list(result) == [1, now]
    finally:
        logger.info('Dropping Kudu table %s ...', kudu_table_name)
        test_table.drop(engine)


@cluster('cdh')
@sdc_min_version('2.7.0.0')
def test_kudu_lookup_apply_default(sdc_builder, sdc_executor, cluster):
    """
    Test when row is found which matches with primary key, but its column that lookup processor needs to return
    doesn't have value.
    When default value is configured, apply the value.

    dev_raw_data_source >> record_deduplicator >> kudu >> trash
    record_deduplicator >> to_error
    """
    if not hasattr(cluster, 'kudu'):
        pytest.skip('Kudu tests only run against clusters with the Kudu service present.')

    tour_de_france_contenders = [dict(favorite_rank=1),
                                 dict(favorite_rank=2)]

    raw_data = ''.join([json.dumps(contender) for contender in tour_de_france_contenders])
    key_columns_mapping = [dict(field='/favorite_rank', columnName='rank')]
    column_to_output_field_mapping = [dict(columnName='name', field='/name', defaultValue=None),
                                      dict(columnName='wins', field='/wins', defaultValue='0')]

    kudu_table_name = get_random_string(string.ascii_letters, 10)
    kudu_master_address = '{}:{}'.format(cluster.server_host, DEFAULT_KUDU_PORT)

    # Build the pipeline.
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data)
    kudu = builder.add_stage('Kudu Lookup',
                             type='processor').set_attributes(kudu_masters=kudu_master_address,
                                                              kudu_table_name=kudu_table_name,
                                                              key_columns_mapping=key_columns_mapping,
                                                              column_to_output_field_mapping=column_to_output_field_mapping,
                                                              case_sensitive=True,
                                                              ignore_missing_value=True)

    record_deduplicator = builder.add_stage('Record Deduplicator')
    to_error = builder.add_stage('To Error')
    trash = builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> kudu >> trash
    record_deduplicator >> to_error

    pipeline = builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    metadata = sqlalchemy.MetaData()
    tdf_contenders_table = sqlalchemy.Table(kudu_table_name,
                                            metadata,
                                            sqlalchemy.Column('rank', sqlalchemy.Integer, primary_key=True),
                                            sqlalchemy.Column('name', sqlalchemy.String),
                                            sqlalchemy.Column('wins', sqlalchemy.Integer),
                                            impala_partition_by='HASH PARTITIONS 16',
                                            impala_stored_as='KUDU',
                                            impala_table_properties={
                                                'kudu.table_name': kudu_table_name,
                                                'kudu.master_addresses': kudu_master_address,
                                                'kudu.num_tablet_replicas': '1'
                                            })

    try:
        logger.info('Creating Kudu table %s ...', kudu_table_name)
        engine = cluster.kudu.engine
        tdf_contenders_table.create(engine)
        conn = engine.connect()
        conn.execute(tdf_contenders_table.insert(), [
                {'rank': 1, 'name': None, 'wins': None},
                {'rank': 2, 'name': None, 'wins': None}])

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)
        for result in snapshot[kudu.instance_name].output:
            if Version(sdc_executor.version) >= Version('3.2.0.0'):
                assert 'name' not in result.value['value']
            else:
                assert result.value['value']['name']['value'] == 'None'
            assert int(result.value['value']['wins']['value']) == 0

    finally:
        logger.info('Dropping Kudu table %s ...', kudu_table_name)
        tdf_contenders_table.drop(engine)


@cluster('cdh')
@sdc_min_version('2.7.0.0')
def test_kudu_lookup_case_sensitive(sdc_builder, sdc_executor, cluster):
    """
    Test the case sensitive option. This pipeline should fail with case sensitive option false
    because the random table name contains uppsercase and lowercase and this pipeline converts
    table name to all lowercase. Therefore table won't be found.

    dev_raw_data_source >> kudu lookup >> trash
    """
    if not hasattr(cluster, 'kudu'):
        pytest.skip('Kudu tests only run against clusters with the Kudu service present.')

    # Generate some data.
    tour_de_france_contenders = [dict(favorite_rank=1),
                                 dict(favorite_rank=2)]

    raw_data = ''.join([json.dumps(contender) for contender in tour_de_france_contenders])
    key_columns_mapping = [dict(field='/favorite_rank', columnName='rank')]
    column_to_output_field_mapping = [dict(columnName='name', field='/name'),
                                      dict(columnName='wins', field='/wins')]

    kudu_table_name = get_random_string(string.ascii_letters, 10)
    kudu_master_address = '{}:{}'.format(cluster.server_host, DEFAULT_KUDU_PORT)

    # Build the pipeline.
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data)
    kudu = builder.add_stage('Kudu Lookup',
                             type='processor').set_attributes(kudu_masters=kudu_master_address,
                                                              kudu_table_name=kudu_table_name,
                                                              key_columns_mapping=key_columns_mapping,
                                                              column_to_output_field_mapping=column_to_output_field_mapping,
                                                              case_sensitive=False,
                                                              ignore_missing_value=True)
    trash = builder.add_stage('Trash')
    dev_raw_data_source >> kudu >> trash

    pipeline = builder.build().configure_for_environment(cluster)
    pipeline.configuration["shouldRetry"] = False
    sdc_executor.add_pipeline(pipeline)

    metadata = sqlalchemy.MetaData()
    tdf_contenders_table = sqlalchemy.Table(kudu_table_name,
                                            metadata,
                                            sqlalchemy.Column('rank', sqlalchemy.Integer, primary_key=True),
                                            sqlalchemy.Column('name', sqlalchemy.String),
                                            sqlalchemy.Column('wins', sqlalchemy.Integer),
                                            impala_partition_by='HASH PARTITIONS 16',
                                            impala_stored_as='KUDU',
                                            impala_table_properties={
                                                'kudu.table_name': kudu_table_name,
                                                'kudu.master_addresses': kudu_master_address,
                                                'kudu.num_tablet_replicas': '1'
                                            })

    try:
        logger.info('Creating Kudu table %s ...', kudu_table_name)
        engine = cluster.kudu.engine
        tdf_contenders_table.create(engine)

        sdc_executor.start_pipeline(pipeline, wait=False)
        sdc_executor.get_pipeline_status(pipeline).wait_for_status('START_ERROR')
        # Test will fail if the pipeline doesn't stop at START_ERROR

    finally:
        logger.info('Dropping Kudu table %s ...', kudu_table_name)
        tdf_contenders_table.drop(engine)


@cluster('cdh')
@sdc_min_version('2.7.0.0')
def test_kudu_lookup_data_types(sdc_builder, sdc_executor, cluster):
    """
    Tests if outgoing records have correct data types and values.
    This test uses a table with a compound key.

    dev_raw_data_source >> record_deduplicator >> kudu >> trash
    record_deduplicator >> to_error
    """
    if not hasattr(cluster, 'kudu'):
        pytest.skip('Kudu tests only run against clusters with the Kudu service present.')

    # Generate some data.
    test_data = [dict(rank=1, name='Chris Froome', wins=100, consecutive=True,
                      prize=1232354385, total_miles=27454, average_speed=536.1),
                 dict(rank=2, name='Greg LeMond', wins=50, consecutive=False,
                      prize=23423958, total_miles=25326, average_speed=500.1),
                 dict(rank=4, name='Vincenzo Nibali', wins=40, consecutive=False,
                      prize=987245, total_miles=13534, average_speed=356.9),
                 dict(rank=3, name='Nairo Quintana', wins=30, consecutive=True,
                      prize=875432, total_miles=13545, average_speed=289.15)]

    tour_de_france_contenders = [dict(favorite_rank=1, name='Chris Froome'),
                                 dict(favorite_rank=2, name='Greg LeMond'),
                                 dict(favorite_rank=4, name='Vincenzo Nibali'),
                                 dict(favorite_rank=3, name='Nairo Quintana'),
                                 dict(favorite_rank=5, name='StreamSets,Inc')]  # This should go to error record

    raw_data = ''.join([json.dumps(contender) for contender in tour_de_france_contenders])
    key_columns_mapping = [dict(field='/favorite_rank', columnName='rank'),
                           dict(field='/name', columnName='name')]

    # Generate different field names than column names in Kudu
    column_to_output_field_mapping = [dict(columnName='wins', field='/wins', defaultValue='0'),
                                      dict(columnName='consecutive', field='/consecutive_2017', defaultValue='false'),
                                      dict(columnName='prize', field='/prize_2017', defaultValue='0'),
                                      dict(columnName='total_miles', field='/total_miles_2017', defaultValue='0'),
                                      dict(columnName='average_speed', field='/avg_speed_2017', defaultValue='0')]

    kudu_table_name = get_random_string(string.ascii_letters, 10)
    kudu_master_address = '{}:{}'.format(cluster.server_host, DEFAULT_KUDU_PORT)

    # Build the pipeline.
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data)
    kudu = builder.add_stage('Kudu Lookup',
                             type='processor').set_attributes(kudu_masters=kudu_master_address,
                                                              kudu_table_name=kudu_table_name,
                                                              key_columns_mapping=key_columns_mapping,
                                                              column_to_output_field_mapping=column_to_output_field_mapping,
                                                              case_sensitive=True,
                                                              ignore_missing_value=True)
    record_deduplicator = builder.add_stage('Record Deduplicator')
    to_error = builder.add_stage('To Error')
    trash = builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> kudu >> trash
    record_deduplicator >> to_error

    pipeline = builder.build().configure_for_environment(cluster)
    pipeline.delivery_guarantee = 'AT_MOST_ONCE'
    sdc_executor.add_pipeline(pipeline)

    metadata = sqlalchemy.MetaData()
    # Impala cannot create a kudu table with decimal and unixtime_micros
    tdf_contenders_table = sqlalchemy.Table(kudu_table_name,
                                            metadata,
                                            sqlalchemy.Column('rank', sqlalchemy.Integer, primary_key=True),
                                            sqlalchemy.Column('name', sqlalchemy.String, primary_key=True),
                                            sqlalchemy.Column('wins', sqlalchemy.Integer),
                                            sqlalchemy.Column('consecutive', sqlalchemy.Boolean),
                                            sqlalchemy.Column('prize', sqlalchemy.BigInteger),
                                            sqlalchemy.Column('total_miles', sqlalchemy.SmallInteger),
                                            sqlalchemy.Column('average_speed', sqlalchemy.Float),
                                            impala_partition_by='HASH PARTITIONS 16',
                                            impala_stored_as='KUDU',
                                            impala_table_properties={
                                                'kudu.table_name': kudu_table_name,
                                                'kudu.master_addresses': kudu_master_address,
                                                'kudu.num_tablet_replicas': '1'
                                            })

    try:
        logger.info('Creating Kudu table %s ...', kudu_table_name)
        engine = cluster.kudu.engine
        tdf_contenders_table.create(engine)
        conn = engine.connect()
        conn.execute(tdf_contenders_table.insert(), test_data)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)
        # Check the data type and value
        for actual, expected in zip(snapshot[kudu.instance_name].output, test_data):
            assert actual.value['value']['name']['value'] == expected['name']
            assert actual.value['value']['wins']['type'] == 'INTEGER'
            assert int(actual.value['value']['wins']['value']) == expected['wins']
            assert actual.value['value']['consecutive_2017']['type'] == 'BOOLEAN'
            assert bool(actual.value['value']['consecutive_2017']['value']) == expected['consecutive']
            assert actual.value['value']['prize_2017']['type'] == 'LONG'
            # Integer is long in Python3
            assert int(actual.value['value']['prize_2017']['value']) == expected['prize']
            assert actual.value['value']['total_miles_2017']['type'] == 'SHORT'
            assert int(actual.value['value']['total_miles_2017']['value']) == expected['total_miles']
            assert actual.value['value']['avg_speed_2017']['type'] == 'FLOAT'
            assert float(actual.value['value']['avg_speed_2017']['value']) == expected['average_speed']

        assert len(snapshot[kudu.instance_name].error_records) == 1
    finally:
        logger.info('Dropping Kudu table %s ...', kudu_table_name)
        tdf_contenders_table.drop(engine)


@cluster('cdh')
@sdc_min_version('2.7.0.0')
def test_kudu_lookup_ignore_missing(sdc_builder, sdc_executor, cluster):
    """
    Test for ignore missing option. Default is true, but when set to false, it sends record to error when
    row is found but its lookup column doesn't have value (null). This sets ignore missing to false and
    checks error record.

    dev_raw_data_source >> record_deduplicator >> kudu >> trash
    record_deduplicator >> to_error
    """
    if not hasattr(cluster, 'kudu'):
        pytest.skip('Kudu tests only run against clusters with the Kudu service present.')

    tour_de_france_contenders = [dict(favorite_rank=1),
                                 dict(favorite_rank=2)]

    raw_data = ''.join([json.dumps(contender) for contender in tour_de_france_contenders])
    key_columns_mapping = [dict(field='/favorite_rank', columnName='rank')]
    column_to_output_field_mapping = [dict(columnName='name', field='/name'),
                                      dict(columnName='wins', field='/wins')]

    kudu_table_name = get_random_string(string.ascii_letters, 10)
    kudu_master_address = '{}:{}'.format(cluster.server_host, DEFAULT_KUDU_PORT)

    # Build the pipeline.
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data)
    kudu = builder.add_stage('Kudu Lookup',
                             type='processor').set_attributes(kudu_masters=kudu_master_address,
                                                              kudu_table_name=kudu_table_name,
                                                              key_columns_mapping=key_columns_mapping,
                                                              column_to_output_field_mapping=column_to_output_field_mapping,
                                                              case_sensitive=True,
                                                              ignore_missing_value=False)
    record_deduplicator = builder.add_stage('Record Deduplicator')
    to_error = builder.add_stage('To Error')
    trash = builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> kudu >> trash
    record_deduplicator >> to_error

    pipeline = builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    metadata = sqlalchemy.MetaData()
    tdf_contenders_table = sqlalchemy.Table(kudu_table_name,
                                            metadata,
                                            sqlalchemy.Column('rank', sqlalchemy.Integer, primary_key=True),
                                            sqlalchemy.Column('name', sqlalchemy.String),
                                            sqlalchemy.Column('wins', sqlalchemy.Integer),
                                            impala_partition_by='HASH PARTITIONS 16',
                                            impala_stored_as='KUDU',
                                            impala_table_properties={
                                                'kudu.table_name': kudu_table_name,
                                                'kudu.master_addresses': kudu_master_address,
                                                'kudu.num_tablet_replicas': '1'
                                            })

    try:
        logger.info('Creating Kudu table %s ...', kudu_table_name)
        engine = cluster.kudu.engine
        tdf_contenders_table.create(engine)
        conn = engine.connect()
        conn.execute(tdf_contenders_table.insert(), [
            {'rank': 1, 'name': 'Chris Froome', 'wins': None},
            {'rank': 2, 'name': 'Greg LeMond', 'wins': None}])

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)
        assert len(snapshot[kudu.instance_name].error_records) == 2

    finally:
        logger.info('Dropping Kudu table %s ...', kudu_table_name)
        tdf_contenders_table.drop(engine)


@cluster('cdh')
@sdc_min_version('3.1.0.0')
def test_kudu_lookup_missing_primary_keys(sdc_builder, sdc_executor, cluster):
    """
    Test if lookup can perform without primary keys.
    dev_raw_data_source >> record_deduplicator >> kudu lookup >> trash
    record_deduplicator >> to_error
    """
    if not hasattr(cluster, 'kudu'):
        pytest.skip('Kudu tests only run against clusters with the Kudu service present.')

    # Perform lookup by a column 'name' which is not primary key and see if other columns can be retrieved.
    tour_de_france_contenders = [dict(name='Chris Froome'),
                                 dict(name='Greg LeMond')]

    raw_data = ''.join([json.dumps(contender) for contender in tour_de_france_contenders])
    key_columns_mapping = [dict(field='/name', columnName='name')]
    column_to_output_field_mapping = [dict(field='/favorite_rank', columnName='rank'),
                                      dict(field='/wins', columnName='wins')]

    kudu_table_name = get_random_string(string.ascii_letters, 10)

    # Build the pipeline.
    kudu_master_address = f'{cluster.server_host}:{DEFAULT_KUDU_PORT}'
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data)
    kudu = builder.add_stage('Kudu Lookup',
                             type='processor').set_attributes(kudu_masters=kudu_master_address,
                                                              kudu_table_name=kudu_table_name,
                                                              key_columns_mapping=key_columns_mapping,
                                                              column_to_output_field_mapping=column_to_output_field_mapping,
                                                              case_sensitive=True,
                                                              ignore_missing_value=False)
    record_deduplicator = builder.add_stage('Record Deduplicator')
    to_error = builder.add_stage('To Error')
    trash = builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> kudu >> trash
    record_deduplicator >> to_error

    pipeline = builder.build().configure_for_environment(cluster)
    pipeline.configuration["shouldRetry"] = False
    sdc_executor.add_pipeline(pipeline)

    metadata = sqlalchemy.MetaData()
    tdf_contenders_table = sqlalchemy.Table(kudu_table_name,
                                            metadata,
                                            sqlalchemy.Column('rank', sqlalchemy.Integer, primary_key=True),
                                            sqlalchemy.Column('name', sqlalchemy.String),
                                            sqlalchemy.Column('wins', sqlalchemy.Integer),
                                            impala_partition_by='HASH PARTITIONS 16',
                                            impala_stored_as='KUDU',
                                            impala_table_properties={
                                                'kudu.table_name': kudu_table_name,
                                                'kudu.master_addresses': kudu_master_address,
                                                'kudu.num_tablet_replicas': '1'
                                            })

    try:
        logger.info('Creating Kudu table %s ...', kudu_table_name)
        engine = cluster.kudu.engine
        tdf_contenders_table.create(engine)
        conn = engine.connect()
        sample_data = [
            {'rank': 1, 'name': 'Chris Froome', 'wins': 5},
            {'rank': 2, 'name': 'Greg LeMond', 'wins': 10}]
        conn.execute(tdf_contenders_table.insert(), sample_data)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # Check the returned values
        for actual, expected in zip(snapshot[kudu.instance_name].output, sample_data):
            assert actual.value['value']['favorite_rank']['value'] == str(expected['rank'])
            assert actual.value['value']['wins']['value'] == str(expected['wins'])
            assert actual.value['value']['name']['value'] == str(expected['name'])
    finally:
        logger.info('Dropping Kudu table %s ...', kudu_table_name)
        tdf_contenders_table.drop(engine)


@cluster('cdh')
def test_solr_destination(sdc_builder, sdc_executor, cluster):
    """Test Solr target pipeline in CDH environment. Note: CDH 5.x at this time runs with Solr 4.x variants.
    This version of Solr does not support API operations for schema and collections.
    """
    test_apache.basic_solr_target('cdh', sdc_builder, sdc_executor, cluster)


@cluster('cdh')
def test_hive_query_executor(sdc_builder, sdc_executor, cluster):
    """Test Hive query executor stage. This is acheived by using a deduplicator which assures us that there is
    only one successful ingest. The pipeline would look like:

        dev_raw_data_source >> record_deduplicator >> hive_query
                                                   >> trash
    """
    hive_table_name = get_random_string(string.ascii_letters, 10)
    hive_cursor = cluster.hive.client.cursor()
    sql_queries = ["CREATE TABLE ${record:value('/text')} (id int, name string)"]

    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=hive_table_name)
    record_deduplicator = builder.add_stage('Record Deduplicator')
    trash = builder.add_stage('Trash')
    hive_query = builder.add_stage('Hive Query', type='executor').set_attributes(sql_queries=sql_queries)

    dev_raw_data_source >> record_deduplicator >> hive_query
    record_deduplicator >> trash

    pipeline = builder.build(title='Hive query executor pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        # assert successful query execution of the pipeline
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)
        assert snapshot[hive_query.instance_name].event_records[0].header['sdc.event.type'] == 'successful-query'

        # assert Hive table creation
        assert hive_cursor.table_exists(hive_table_name)

        # Re-running the same query to create Hive table should fail the query. So assert the failure.
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)
        assert snapshot[hive_query.instance_name].event_records[0].header['sdc.event.type'] == 'failed-query'
    finally:
        # drop the Hive table
        hive_cursor.execute(f'DROP TABLE `{hive_table_name}`')


@cluster('cdh')
def test_mapreduce_executor(sdc_builder, sdc_executor, cluster):
    """Test MapReduce executor stage. This is acheived by using a deduplicator which assures us that there is
    only one successful ingest and that we ingest to HDFS. The executor then triggers MapReduce job which should
    convert the ingested HDFS Avro data to Parquet. The pipeline would look like:

        dev_raw_data_source >> record_deduplicator >> hadoop_fs >= mapreduce
                                                   >> trash
    """
    hdfs_directory = '/tmp/out/{}'.format(get_random_string(string.ascii_letters, 10))
    product_data = [dict(name='iphone', price=649.99),
                    dict(name='pixel', price=649.89)]
    raw_data = ''.join([json.dumps(product) for product in product_data])
    avro_schema = ('{ "type" : "record", "name" : "STF", "fields" : '
                   '[ { "name" : "name", "type" : "string" }, { "name" : "price", "type" : "double" } ] }')

    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data)
    record_deduplicator = builder.add_stage('Record Deduplicator')
    trash = builder.add_stage('Trash')
    hadoop_fs = builder.add_stage('Hadoop FS', type='destination')
    # max_records_in_file enables to close the file and generate the event
    hadoop_fs.set_attributes(avro_schema=avro_schema, avro_schema_location='INLINE', data_format='AVRO',
                             directory_template=hdfs_directory, files_prefix='sdc-${sdc:id()}', max_records_in_file=1)
    mapreduce = builder.add_stage('MapReduce', type='executor')
    mapreduce.job_type = 'AVRO_PARQUET'
    mapreduce.output_directory = hdfs_directory

    dev_raw_data_source >> record_deduplicator >> hadoop_fs >= mapreduce
    record_deduplicator >> trash

    pipeline = builder.build(title='MapReduce executor pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # assert events (MapReduce) generated
        assert len(snapshot[mapreduce.instance_name].event_records) == len(product_data)

        # make sure MapReduce job is done and is successful
        for event in snapshot[mapreduce.instance_name].event_records:
            job_id = event.value['value']['job-id']['value']
            assert cluster.yarn.wait_for_job_to_end(job_id) == 'SUCCEEDED'

        # assert parquet data is same as what is ingested
        for event in snapshot[hadoop_fs.instance_name].event_records:
            file_path = event.value['value']['filepath']['value']
            hdfs_parquet_file_path = '{}.parquet'.format(file_path)
            hdfs_data = cluster.hdfs.get_data_from_parquet(hdfs_parquet_file_path)
            assert hdfs_data[0] in product_data
    finally:
        # remove HDFS files
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)


@cluster('cdh')
def test_spark_executor(sdc_builder, sdc_executor, cluster):
    """Test Spark executor stage. This is acheived by using 2 pipelines. The 1st pipeline would generate the
    application resource file (Python in this case) which will be used by the 2nd pipeline for spark-submit. Spark
    executor will do the spark-submit and we assert that it has submitted the job to Yarn. The pipelines would
    look like:

        dev_raw_data_source >> local_fs >= pipeline_finisher_executor

        dev_raw_data_source >> record_deduplicator >> spark_executor
                               record_deduplicator >> trash
    """
    python_data = 'print("Hello World!")'
    tmp_directory = '/tmp/out/{}'.format(get_random_string(string.ascii_letters, 10))
    python_suffix = 'py'
    application_name = ''.join(['stf_', get_random_string(string.ascii_letters, 10)])

    # build the 1st pipeline - file generator
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=python_data)
    local_fs = builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT', directory_template=tmp_directory,
                            files_prefix='sdc-${sdc:id()}', files_suffix=python_suffix, max_records_in_file=1)
    # we use the finisher so as local_fs can generate event with file_path being generated
    pipeline_finisher_executor = builder.add_stage('Pipeline Finisher Executor')

    dev_raw_data_source >> local_fs >= pipeline_finisher_executor

    pipeline = builder.build(title='To File pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    # run the pipeline and capture the file path
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    file_path = snapshot[local_fs.instance_name].event_records[0].value['value']['filepath']['value']

    # build the 2nd pipeline - spark executor
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data='dummy')
    record_deduplicator = builder.add_stage('Record Deduplicator')
    trash = builder.add_stage('Trash')
    spark_executor = builder.add_stage('Spark Executor', type='executor')
    spark_executor.set_attributes(cluster_manager='YARN',
                                  minimum_number_of_worker_nodes=1,
                                  maximum_number_of_worker_nodes=1,
                                  application_name=application_name,
                                  deploy_mode='CLUSTER',
                                  driver_memory='10m',
                                  executor_memory='10m',
                                  application_resource=file_path,
                                  language='PYTHON')

    dev_raw_data_source >> record_deduplicator >> spark_executor
    record_deduplicator >> trash

    pipeline = builder.build(title='Spark executor pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
    sdc_executor.stop_pipeline(pipeline)

    # assert Spark executor has triggered the YARN job
    assert cluster.yarn.wait_for_app_to_register(application_name)
