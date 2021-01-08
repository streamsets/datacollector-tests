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

import json
import logging
import string
import time
from datetime import datetime
from decimal import Decimal

import pytest
import sqlalchemy
from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DEFAULT_KUDU_PORT = 7051


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
                             type='destination').set_attributes(table_name='{}.{}'.format('impala::default',
                                                                                          kudu_table_name),
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

    if Version(cluster.version) < Version('cdh5.12.0'):
        pytest.skip('Test requires CDH 5.12.0+ to run')

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
                             type='destination').set_attributes(table_name='{}.{}'.format('impala::default',
                                                                                          kudu_table_name),
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
@sdc_min_version('3.6.0')
def test_kudu_destination_decimal_type(sdc_builder, sdc_executor, cluster):
    """Simple Dev Raw Data Source to Kudu pipeline inserting column of decimal type and checking later on
    decimal type is correctly stored by querying Kudu database

    dev_raw_data_source >> kudu
    """
    if not hasattr(cluster, 'kudu'):
        pytest.skip('Kudu tests only run against clusters with the Kudu service present.')

    if not Version(cluster.kudu.version) >= Version('1.7.0'):
        pytest.skip(f'Test only designed to run on Kudu version >= 1.7.0, but found {cluster.kudu.version}')

    # Generate some data.
    tour_de_france_contenders = [dict(favorite_rank=1, name='Chris Froome', wins=3, weight=153.22),
                                 dict(favorite_rank=2, name='Greg LeMond', wins=3, weight=158.73),
                                 dict(favorite_rank=4, name='Vincenzo Nibali', wins=1, weight=144),
                                 dict(favorite_rank=3, name='Nairo Quintana', wins=0, weight=165.34)]
    raw_data = '\n'.join([json.dumps(contender) for contender in tour_de_france_contenders])

    field_to_column_mapping = [dict(field='/favorite_rank', columnName='rank'),
                               dict(field='/name', columnName='name'),
                               dict(field='/wins', columnName='wins'),
                               dict(field='/weight', columnName='weight')]

    kudu_table_name = get_random_string(string.ascii_letters, 10)
    kudu_master_address = '{}:{}'.format(cluster.server_host, DEFAULT_KUDU_PORT)

    # Build the pipeline.
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data)
    kudu = builder.add_stage('Kudu',
                             type='destination').set_attributes(table_name='{}.{}'.format('impala::default',
                                                                                          kudu_table_name),
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
                                            sqlalchemy.Column('weight', sqlalchemy.DECIMAL(5, 2)),
                                            impala_partition_by='HASH PARTITIONS 16',
                                            impala_stored_as='KUDU',
                                            impala_table_properties={
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
        result_list = list(result)

        sorted_tour_de_france_contenders = [tuple([item['favorite_rank'], item['name'], item['wins'],
                                                   round(Decimal(item['weight']), 2)])
                                            for item in sorted(tour_de_france_contenders,
                                                               key=lambda key: key['favorite_rank'])]

        assert result_list == sorted_tour_de_france_contenders

    finally:
        logger.info('Dropping Kudu table %s ...', kudu_table_name)
        tdf_contenders_table.drop(engine)
