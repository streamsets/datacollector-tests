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
from decimal import Decimal

import pytest
import sqlalchemy
from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

logger = logging.getLogger(__name__)

DEFAULT_KUDU_PORT = 7051


@cluster('cdh')
@sdc_min_version('2.7.0.0')
def test_kudu_lookup_apply_default(sdc_builder, sdc_executor, cluster):
    """
    Test when row is found which matches with primary key, but its column that lookup processor needs to return
    doesn't have value.
    When default value is configured, apply the value.

    dev_raw_data_source >> kudu >> trash
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
    kudu_master_address = f'{cluster.server_host}:{DEFAULT_KUDU_PORT}'

    # Build the pipeline.
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data,
                                                                                  stop_after_first_batch=True)

    kudu = builder.add_stage('Kudu Lookup',type='processor')
    kudu.set_attributes(kudu_table_name=f'impala::default.{kudu_table_name}',
                        key_columns_mapping=key_columns_mapping,
                        column_to_output_field_mapping=column_to_output_field_mapping,
                        case_sensitive=True,
                        ignore_missing_value=True)

    if Version(sdc_builder.version) < Version("6.1.0"):
        kudu.set_attributes(kudu_masters=kudu_master_address)
    else:
        kudu.set_attributes(kudu_primary_nodes=kudu_master_address)


    wiretap = builder.add_wiretap()

    dev_raw_data_source >> kudu >> wiretap.destination

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

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        for result in wiretap.output_records:
            if Version(sdc_executor.version) >= Version('3.2.0.0'):
                assert 'name' not in result.field
            else:
                assert result.field['name'].value == 'None'
            assert int(result.field['wins'].value) == 0

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

    kudu = builder.add_stage('Kudu Lookup',type='processor')
    kudu.set_attributes(kudu_table_name=f'impala::default.{kudu_table_name}',
                        key_columns_mapping=key_columns_mapping,
                        column_to_output_field_mapping=column_to_output_field_mapping,
                        case_sensitive=False,
                        ignore_missing_value=True)

    if Version(sdc_builder.version) < Version("6.1.0"):
        kudu.set_attributes(kudu_masters=kudu_master_address)
    else:
        kudu.set_attributes(kudu_primary_nodes=kudu_master_address)

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

    dev_raw_data_source >> kudu >> wiretap
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
                                                                                  raw_data=raw_data,
                                                                                  stop_after_first_batch=True)

    kudu = builder.add_stage('Kudu Lookup',type='processor')
    kudu.set_attributes(kudu_table_name=f'impala::default.{kudu_table_name}',
                        key_columns_mapping=key_columns_mapping,
                        column_to_output_field_mapping=column_to_output_field_mapping,
                        case_sensitive=True,
                        ignore_missing_value=True)

    if Version(sdc_builder.version) < Version("6.1.0"):
        kudu.set_attributes(kudu_masters=kudu_master_address)
    else:
        kudu.set_attributes(kudu_primary_nodes=kudu_master_address)

    wiretap = builder.add_wiretap()

    dev_raw_data_source >> kudu >> wiretap.destination

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
                                                'kudu.master_addresses': kudu_master_address,
                                                'kudu.num_tablet_replicas': '1'
                                            })

    try:
        logger.info('Creating Kudu table %s ...', kudu_table_name)
        engine = cluster.kudu.engine
        tdf_contenders_table.create(engine)
        conn = engine.connect()
        conn.execute(tdf_contenders_table.insert(), test_data)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Check the data type and value
        for actual, expected in zip(wiretap.output_records, test_data):
            assert actual.field['name'].value == expected['name']
            assert actual.field['wins'].type == 'INTEGER'
            assert int(actual.field['wins'].value) == expected['wins']
            assert actual.field['consecutive_2017'].type == 'BOOLEAN'
            assert bool(actual.field['consecutive_2017'].value) == expected['consecutive']
            assert actual.field['prize_2017'].type == 'LONG'
            # Integer is long in Python3
            assert int(actual.field['prize_2017'].value) == expected['prize']
            assert actual.field['total_miles_2017'].type == 'SHORT'
            assert int(actual.field['total_miles_2017'].value) == expected['total_miles']
            assert actual.field['avg_speed_2017'].type == 'FLOAT'
            assert float(actual.field['avg_speed_2017'].value) == expected['average_speed']

        assert len(wiretap.error_records) == 1
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

    dev_raw_data_source >> kudu >> wiretap
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
    kudu_master_address = f'{cluster.server_host}:{DEFAULT_KUDU_PORT}'

    # Build the pipeline.
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data,
                                                                                  stop_after_first_batch=True)

    kudu = builder.add_stage('Kudu Lookup',type='processor')
    kudu.set_attributes(kudu_table_name=f'impala::default.{kudu_table_name}',
                        key_columns_mapping=key_columns_mapping,
                        column_to_output_field_mapping=column_to_output_field_mapping,
                        case_sensitive=True,
                        ignore_missing_value=False)

    if Version(sdc_builder.version) < Version("6.1.0"):
        kudu.set_attributes(kudu_masters=kudu_master_address)
    else:
        kudu.set_attributes(kudu_primary_nodes=kudu_master_address)

    wiretap = builder.add_wiretap()

    dev_raw_data_source >> kudu >> wiretap.destination

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

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert len(wiretap.error_records) == 2

    finally:
        logger.info('Dropping Kudu table %s ...', kudu_table_name)
        tdf_contenders_table.drop(engine)


@cluster('cdh')
@sdc_min_version('3.1.0.0')
def test_kudu_lookup_missing_primary_keys(sdc_builder, sdc_executor, cluster):
    """
    Test if lookup can perform without primary keys.
    dev_raw_data_source >> kudu lookup >> wiretap
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
                                                                                  raw_data=raw_data,
                                                                                  stop_after_first_batch=True)

    kudu = builder.add_stage('Kudu Lookup',type='processor')
    kudu.set_attributes(kudu_table_name=f'impala::default.{kudu_table_name}',
                        key_columns_mapping=key_columns_mapping,
                        column_to_output_field_mapping=column_to_output_field_mapping,
                        case_sensitive=True,
                        ignore_missing_value=False)

    if Version(sdc_builder.version) < Version("6.1.0"):
        kudu.set_attributes(kudu_masters=kudu_master_address)
    else:
        kudu.set_attributes(kudu_primary_nodes=kudu_master_address)

    wiretap = builder.add_wiretap()

    dev_raw_data_source >> kudu >> wiretap.destination

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

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        # Check the returned values
        for actual, expected in zip(wiretap.output_records, sample_data):
            assert actual.field['favorite_rank'].value == expected['rank']
            assert actual.field['wins'].value == expected['wins']
            assert actual.field['name'].value == str(expected['name'])
    finally:
        logger.info('Dropping Kudu table %s ...', kudu_table_name)
        tdf_contenders_table.drop(engine)


@cluster('cdh')
@sdc_min_version('3.6.0')
def test_kudu_lookup_decimal_type(sdc_builder, sdc_executor, cluster):
    """
    After inserting rows in a Kudu table containing a decimal type column check that decimal type column is correctly
    retrieved by Kudu processor

    dev_raw_data_source >> kudu >> wiretap
    """
    if not hasattr(cluster, 'kudu'):
        pytest.skip('Kudu tests only run against clusters with the Kudu service present.')

    if not Version(cluster.kudu.version) >= Version('1.7.0'):
        pytest.skip(f'Test only designed to run on Kudu version >= 1.7.0, but found {cluster.kudu.version}')

    tour_de_france_contenders = [dict(rank=1, weight=150.58),
                                 dict(rank=2, weight=140.11)]

    raw_data = ''.join([json.dumps(contender) for contender in tour_de_france_contenders])
    key_columns_mapping = [dict(field='/rank', columnName='rank')]
    column_to_output_field_mapping = [dict(columnName='rank', field='/rank'),
                                      dict(columnName='weight', field='/weight', defaultValue='0')]

    kudu_table_name = get_random_string(string.ascii_letters, 10)
    kudu_master_address = f'{cluster.server_host}:{DEFAULT_KUDU_PORT}'

    # Build the pipeline.
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data,
                                                                                  stop_after_first_batch=True)

    kudu = builder.add_stage('Kudu Lookup',type='processor')
    kudu.set_attributes(kudu_table_name=f'impala::default.{kudu_table_name}',
                        key_columns_mapping=key_columns_mapping,
                        column_to_output_field_mapping=column_to_output_field_mapping,
                        case_sensitive=True,
                        ignore_missing_value=True)

    if Version(sdc_builder.version) < Version("6.1.0"):
        kudu.set_attributes(kudu_masters=kudu_master_address)
    else:
        kudu.set_attributes(kudu_primary_nodes=kudu_master_address)

    wiretap = builder.add_wiretap()

    dev_raw_data_source >> kudu >> wiretap.destination

    pipeline = builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    metadata = sqlalchemy.MetaData()
    tdf_contenders_table = sqlalchemy.Table(kudu_table_name,
                                            metadata,
                                            sqlalchemy.Column('rank', sqlalchemy.Integer, primary_key=True),
                                            sqlalchemy.Column('weight', sqlalchemy.DECIMAL(5,2)),
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
        conn = engine.connect()
        conn.execute(tdf_contenders_table.insert(), tour_de_france_contenders)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        i = 0
        for result in wiretap.output_records:
            assert result.field['weight'].value == round(Decimal(tour_de_france_contenders[i]['weight']), 2)
            i += 1

    finally:
        logger.info('Dropping Kudu table %s ...', kudu_table_name)
        tdf_contenders_table.drop(engine)
