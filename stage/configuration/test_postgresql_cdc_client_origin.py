# Copyright 2021 StreamSets Inc.
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
from collections import namedtuple

import pytest
import sqlalchemy
from sqlalchemy import DateTime, Time, Date
from datetime import datetime, date, time
import pytz
from streamsets.testframework.utils import get_random_string
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import database, category, sdc_min_version


logger = logging.getLogger(__name__)

PRIMARY_KEY = 'id'
NAME_COLUMN = 'name'
TEST_DATETIME_TZ = 'test_datetime_tz'
TEST_DATETIME = 'test_datetime'
TEST_DATE = 'test_date'
TEST_TIME_TZ = 'test_time_tz'
TEST_TIME = 'test_time'

OperationsData = namedtuple('OperationsData', ['kind', 'table', 'columnnames', 'columnvalues', 'oldkeys'])
Oldkeys = namedtuple('Oldkeys', ['keynames', 'keyvalues'])


INSERT_ROWS = [
    {PRIMARY_KEY: 0, NAME_COLUMN: 'Mbappe', TEST_DATETIME_TZ: '09-10-2017 10:10:10', TEST_DATETIME: '09-09-2017 10:10:10', TEST_TIME_TZ: '10:10:10', TEST_TIME: '10:10:10', TEST_DATE: '09-11-2017'},
    {PRIMARY_KEY: 1, NAME_COLUMN: 'Kane', TEST_DATETIME_TZ: '09-30-2017 10:10:10', TEST_DATETIME: '09-30-2017 10:20:20', TEST_TIME_TZ: '10:20:10', TEST_TIME: '10:20:10', TEST_DATE: '09-30-2017'},
    {PRIMARY_KEY: 2, NAME_COLUMN: 'Griezmann', TEST_DATETIME_TZ: '09-10-2017 10:30:10', TEST_DATETIME: '09-09-2017 10:30:10', TEST_TIME_TZ: '10:30:10', TEST_TIME: '10:30:10', TEST_DATE: '09-11-2017'},
]

KIND_FOR_INSERT = 'insert'

CHECK_REP_SLOT_QUERY = 'select slot_name from pg_replication_slots;'

POLL_INTERVAL = "${1 * SECONDS}"

pytestmark = [pytest.mark.sdc_min_version('3.16.0'), pytest.mark.database('postgresql')]

@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'add_unsupported_fields': False}, {'add_unsupported_fields': True}])
def test_add_unsupported_fields(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
def test_additional_jdbc_configuration_properties(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
def test_batch_wait_time_in_ms(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_connection_health_test_query(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_connection_timeout_in_seconds(sdc_builder, sdc_executor, database):
    pass


@database('postgresql')
@sdc_min_version('4.4.0')
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'parse_datetimes': True, 'database_time_zone': 'UTC'},
                                              {'parse_datetimes': False}])
def test_parse_datetimes(sdc_builder, sdc_executor, database, stage_attributes):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL with CDC enabled.')
    table_name = get_random_string(string.ascii_lowercase, 20)
    start_date = '09-09-2017 10:10:20'

    date_time_format = '%m-%d-%Y %H:%M:%S'
    date_format = '%m-%d-%Y'
    time_format = '%H:%M:%S'

    db_date_output_format = '%Y-%m-%d'
    db_date_time_input_format = '%m-%d-%Y %H:%M:%S'
    db_date_time_output_format = '%Y-%m-%d %H:%M:%S'
    db_date_time_tz_output_format = '%Y-%m-%d %H:%M:%S%z'
    db_time_output_format = '%H:%M:%S'
    db_time_tz_output_format = '%H:%M:%S%z'

    replication_slot = get_random_string(string.ascii_lowercase, 10)
    stage_attributes.update({'max_batch_size_in_records': 1, 'replication_slot': replication_slot,
                             'poll_interval': POLL_INTERVAL, 'initial_change': 'DATE',
                             'start_date': start_date})
    try:
        # Create table and then perform some operations to simulate activity
        table = _create_table_in_database(table_name, database)

        stage_attributes.update({'tables': [{
                                     "table": table_name
                                 }]})

        connection = database.engine.connect()

        postgres_cdc_client, pipeline, wiretap = get_postgres_cdc_client_to_wiretap_pipeline(sdc_builder, database, stage_attributes)

        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)

        _insert(connection=connection, table=table)

        record_data_test_date = []
        record_data_test_datetime = []
        record_data_test_datetime_tz = []
        record_data_test_time = []
        record_data_test_time_tz = []
        for record in wiretap.output_records:
            record_change_data = record.get_field_data('change')
            for change in record_change_data:
                columnnames = change.get('columnnames')
                if columnnames is not None:
                    columnindex = columnnames.index('test_date')
                    record_data_test_date.append(('test_date', change.get('columnvalues')[columnindex]))
                    columnindex = columnnames.index('test_datetime')
                    record_data_test_datetime.append(('test_datetime', change.get('columnvalues')[columnindex]))
                    columnindex = columnnames.index('test_datetime_tz')
                    record_data_test_datetime_tz.append(('test_datetime_tz', change.get('columnvalues')[columnindex]))
                    columnindex = columnnames.index('test_time')
                    record_data_test_time.append(('test_time', change.get('columnvalues')[columnindex]))
                    columnindex = columnnames.index('test_time_tz')
                    record_data_test_time_tz.append(('test_time_tz', change.get('columnvalues')[columnindex]))

        sdc_executor.stop_pipeline(pipeline=pipeline).wait_for_stopped(timeout_sec=60)

        if stage_attributes['parse_datetimes']:
            assert record_data_test_date == [('test_date', datetime.strptime(row['test_date'], date_format))
                                            for row in INSERT_ROWS]
            assert record_data_test_datetime == [('test_datetime', datetime.strptime(row['test_datetime'], date_time_format))
                                                for row in INSERT_ROWS]
            assert record_data_test_datetime_tz == [('test_datetime_tz', datetime.strptime(row['test_datetime_tz']
                                                      , date_time_format).replace(tzinfo=pytz.utc).isoformat().replace('+00:00', 'Z'))
                                                    for row in INSERT_ROWS]
            assert record_data_test_time == [('test_time', datetime.strptime(row['test_time'], time_format).replace(year=1970))
                                              for row in INSERT_ROWS]
            assert record_data_test_time_tz == [('test_time_tz', datetime.strptime(row['test_time_tz']
                                                , time_format).replace(year=1970))
                                               for row in INSERT_ROWS]
        else:
            assert record_data_test_date == [('test_date', datetime.strptime(row['test_date'], date_format)
                                            .strftime(db_date_output_format)) for row in INSERT_ROWS]
            assert record_data_test_datetime == [('test_datetime', datetime.strptime(row['test_datetime']
                                                  , db_date_time_input_format) .strftime(db_date_time_output_format))
                                                  for row in INSERT_ROWS]
            assert record_data_test_datetime_tz == [('test_datetime_tz', datetime.strptime(row['test_datetime_tz']
                                                    , db_date_time_input_format)
                                                    .replace(tzinfo=pytz.utc).strftime(db_date_time_tz_output_format)[0:22])
                                                    for row in INSERT_ROWS]
            assert record_data_test_time == [('test_time', datetime.strptime(row['test_time'], time_format)
                                            .replace(year=1970).strftime(db_time_output_format))
                                            for row in INSERT_ROWS]
            assert record_data_test_time_tz == [('test_time_tz', datetime.strptime(row['test_time_tz'], time_format)
                                                .replace(year=1970).replace(tzinfo=pytz.utc)
                                                .strftime(db_time_tz_output_format)[0:11])
                                                for row in INSERT_ROWS]

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        if table is not None:
            table.drop(database.engine)
            logger.info('Table: %s dropped.', table_name)
        database.deactivate_and_drop_replication_slot(replication_slot)
        sdc_executor.remove_pipeline(pipeline)


@stub
@category('basic')
def test_database_time_zone(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'enforce_read_only_connection': False},
                                              {'enforce_read_only_connection': True}])
def test_enforce_read_only_connection(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
def test_idle_timeout_in_seconds(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_init_query(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'initial_change': 'DATE'},
                                              {'initial_change': 'LATEST'},
                                              {'initial_change': 'LSN'}])
def test_initial_change(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
def test_jdbc_connection_string(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_jdbc_driver_class_name(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
def test_max_batch_size_in_records(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_max_connection_lifetime_in_seconds(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_maximum_pool_size(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_minimum_idle_connections(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
def test_operations(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_password(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
def test_poll_interval(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
def test_query_timeout(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'remove_replication_slot_on_close': False},
                                              {'remove_replication_slot_on_close': True}])
def test_remove_replication_slot_on_close(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
def test_replication_slot(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'initial_change': 'DATE'}])
def test_start_date(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'initial_change': 'LSN'}])
def test_start_lsn(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
def test_tables(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'transaction_isolation': 'DEFAULT'},
                                              {'transaction_isolation': 'TRANSACTION_READ_COMMITTED'},
                                              {'transaction_isolation': 'TRANSACTION_READ_UNCOMMITTED'},
                                              {'transaction_isolation': 'TRANSACTION_REPEATABLE_READ'},
                                              {'transaction_isolation': 'TRANSACTION_SERIALIZABLE'}])
def test_transaction_isolation(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'unsupported_field_type': 'DISCARD'},
                                              {'unsupported_field_type': 'SEND_TO_PIPELINE'},
                                              {'unsupported_field_type': 'TO_ERROR'}])
def test_unsupported_field_type(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': False}, {'use_credentials': True}])
def test_use_credentials(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_username(sdc_builder, sdc_executor, database, stage_attributes):
    pass


# Util functions

def _create_table_in_database(table_name, database):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column(PRIMARY_KEY, sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(NAME_COLUMN, sqlalchemy.String(20)),
        sqlalchemy.Column(TEST_DATETIME_TZ, DateTime(timezone=True)),
        sqlalchemy.Column(TEST_DATETIME, DateTime(timezone=False)),
        sqlalchemy.Column(TEST_TIME_TZ, Time(timezone=True)),
        sqlalchemy.Column(TEST_TIME, Time(timezone=False)),
        sqlalchemy.Column(TEST_DATE, Date()))
    logger.info('Creating table %s in %s database ...', table_name, database.type)
    table.create(database.engine)
    return table

def _insert(connection, table):
    connection.execute(table.insert(), INSERT_ROWS)

    # Prepare expected data to compare for verification against snapshot data.
    operations_data = []
    for row in INSERT_ROWS:
        operations_data.append(OperationsData(KIND_FOR_INSERT,
                                              table.name,
                                              [PRIMARY_KEY, NAME_COLUMN, TEST_DATETIME_TZ, TEST_DATETIME, TEST_DATE, TEST_TIME_TZ, TEST_TIME],
                                              list(row.values()),
                                              None))  # No oldkeys expected.
    return operations_data

def get_postgres_cdc_client_to_wiretap_pipeline(sdc_builder, database, stage_attributes,
                                              configure_for_environment_flag=True):
    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
    postgres_cdc_client.set_attributes(**stage_attributes)
    wiretap = pipeline_builder.add_wiretap()
    postgres_cdc_client >> wiretap.destination
    if configure_for_environment_flag:
        pipeline = pipeline_builder.build().configure_for_environment(database)
    else:
        pipeline = pipeline_builder.build()
    return postgres_cdc_client, pipeline, wiretap
