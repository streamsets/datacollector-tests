import copy
import logging
import string

import pytest
import sqlalchemy
from sqlalchemy import Column, Integer, String, CHAR, DateTime
from streamsets.testframework.markers import credentialstore, database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__file__)

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Manish'},
    {'id': 2, 'name': 'Shravan'},
    {'id': 3, 'name': 'Shubham'}
]


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_additional_jdbc_configuration_properties(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('auto_commit', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_auto_commit(sdc_builder, sdc_executor, auto_commit):
    pass


@pytest.mark.parametrize('per_batch_strategy', ['SWITCH_TABLES'])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_batches_from_result_set(sdc_builder, sdc_executor, per_batch_strategy):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_connection_health_test_query(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_connection_timeout_in_seconds(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('convert_timestamp_to_string', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_convert_timestamp_to_string(sdc_builder, sdc_executor, convert_timestamp_to_string):
    pass


@pytest.mark.parametrize('data_time_zone', ['Asia/Kolkata', 'UTC', 'US/Samoa'])
@database
def test_jdbc_multitable_consumer_origin_configuration_data_time_zone(sdc_builder, sdc_executor, data_time_zone,
                                                                      database):
    """Check if the JDBC Multitable Consumer considers Data Time Zone while
    evaluating the condition for offset column based conditions.
    Conditions are adjusted for the three time zone so that it should be evaluated to 2019-07-11 07:00:00 UTC.
    Time Zones -
        Asia/Kolkata - UTC time + 5:30
        UTC -  UTC time
        US/Samoa - UTC time - 11:00
    e.g. if we checking condition last_updated > 2019-07-11 12:30:10
    """
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, 20))
    rows_in_database = [{'id': 1, 'name': 'Skip This', 'last_updated': '2019-07-11 07:00:00'},
                        {'id': 2, 'name': 'Manish', 'last_updated': '2019-07-11 07:31:10'},
                        {'id': 3, 'name': 'Shravan', 'last_updated': '2019-07-11 07:32:10'},
                        {'id': 4, 'name': 'Shubham', 'last_updated': '2019-07-11 07:33:10'}]
    try:
        # Create tables needed for table and insert data
        columns = [Column('id', Integer, primary_key=True), Column('name', String(32)),
                   Column('last_updated', DateTime(timezone=True))]
        table = create_table(database, columns, table_name)
        insert_data_in_table(database, table, rows_in_database)
        condition = ''
        if data_time_zone == 'Asia/Kolkata':
            condition = 'last_updated > \'${time:extractStringFromDate(time:extractDateFromString("2019-07-11 12:30:10", "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")}\''
        elif data_time_zone == 'UTC':
            condition = 'last_updated > \'${time:extractStringFromDate(time:extractDateFromString("2019-07-11 07:30:00", "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")}\''
        elif data_time_zone == 'US/Samoa':
            condition = 'last_updated > \'${time:extractStringFromDate(time:extractDateFromString("2019-07-10 20:30:00", "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")}\''

        # Build the pipeline
        attributes = {'data_time_zone': data_time_zone,
                      'table_configs': [{'tablePattern': f'%{src_table_prefix}%',
                                         'overrideDefaultOffsetColumns': True,
                                         'offsetColumns': ['last_updated'],
                                         'extraOffsetColumnConditions': condition}]}
        jdbc_multitable_consumer, pipeline = get_jdbc_multitable_consumer_to_trash_pipeline(sdc_builder, database,
                                                                                            attributes)

        # Execute pipeline and get the snapshot
        snapshot = execute_pipeline(sdc_executor, pipeline, 1)

        # Column names are converted to lower case since Oracle database column names are in upper case.
        tuples_to_lower_name = lambda tup: (tup[0].lower(), str(tup[1]))
        rows_from_snapshot = [tuples_to_lower_name(list(record.field.items())[1])
                              for record in snapshot_content(snapshot, jdbc_multitable_consumer)]
        assert rows_from_snapshot == [('name', row['name']) for row in rows_in_database][1:]
    finally:
        sdc_executor.stop_pipeline(pipeline)
        delete_table([table], database)


@pytest.mark.parametrize('enforce_read_only_connection', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_enforce_read_only_connection(sdc_builder, sdc_executor, enforce_read_only_connection):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_fetch_size(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_idle_timeout_in_seconds(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_init_query(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('initial_table_order_strategy', ['ALPHABETICAL', 'NONE', 'REFERENTIAL_CONSTRAINTS'])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_initial_table_order_strategy(sdc_builder, sdc_executor, initial_table_order_strategy):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_jdbc_connection_string(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_jdbc_driver_class_name(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_max_batch_size_in_records(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_max_blob_size_in_bytes(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_max_clob_size_in_characters(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_max_connection_lifetime_in_seconds(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_maximum_pool_size(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_minimum_idle_connections(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_new_table_discovery_interval(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_no_more_data_event_generation_delay_in_seconds(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_number_of_retries_on_sql_error(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_number_of_threads(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('on_record_error', ['DISCARD', 'STOP_PIPELINE', 'TO_ERROR'])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_on_record_error(sdc_builder, sdc_executor, on_record_error):
    pass


@pytest.mark.parametrize('on_unknown_type', ['CONVERT_TO_STRING', 'STOP_PIPELINE'])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_on_unknown_type(sdc_builder, sdc_executor, on_unknown_type):
    pass


@pytest.mark.parametrize('use_credentials', [True])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_password(sdc_builder, sdc_executor, use_credentials):
    pass


@pytest.mark.parametrize('per_batch_strategy', ['PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE', 'SWITCH_TABLES'])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_per_batch_strategy(sdc_builder, sdc_executor, per_batch_strategy):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_queries_per_second(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('quote_character', ['BACKTICK', 'DOUBLE_QUOTES', 'NONE'])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_quote_character(sdc_builder, sdc_executor, quote_character):
    pass


@pytest.mark.parametrize('per_batch_strategy', ['SWITCH_TABLES'])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_result_set_cache_size(sdc_builder, sdc_executor, per_batch_strategy):
    pass


@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_table_configs(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('transaction_isolation', ['DEFAULT', 'TRANSACTION_READ_COMMITTED', 'TRANSACTION_READ_UNCOMMITTED', 'TRANSACTION_REPEATABLE_READ', 'TRANSACTION_SERIALIZABLE'])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_transaction_isolation(sdc_builder, sdc_executor, transaction_isolation):
    pass


@pytest.mark.parametrize('use_credentials', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_use_credentials(sdc_builder, sdc_executor, use_credentials):
    pass


@pytest.mark.parametrize('use_credentials', [True])
@pytest.mark.skip('Not yet implemented')
def test_jdbc_multitable_consumer_origin_configuration_username(sdc_builder, sdc_executor, use_credentials):
    pass


# Util functions

def create_table(database, columns, table_name):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        *columns
    )
    logger.info('Creating table %s in %s database ...', table_name, database.type)
    table.create(database.engine)
    return table


def get_jdbc_multitable_consumer_to_trash_pipeline(sdc_builder, database, attributes):
    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(**attributes)
    trash = pipeline_builder.add_stage('Trash')
    jdbc_multitable_consumer >> trash
    pipeline = pipeline_builder.build().configure_for_environment(database)
    return jdbc_multitable_consumer, pipeline


def execute_pipeline(sdc_executor, pipeline, number_of_batches=1, snapshot_batch_size=10):
    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True, batches=number_of_batches,
                                             batch_size=snapshot_batch_size).snapshot
    return snapshot


def insert_data_in_table(database, table, rows_to_insert):
    logger.info('Adding three rows into %s database ...', database.type)
    connection = database.engine.connect()
    connection.execute(table.insert(), rows_to_insert)


def snapshot_content(snapshot, jdbc_multitable_consumer):
    """This is common function can be used at in many TCs to get snapshot content."""
    processed_data = []
    for snapshot_batch in snapshot.snapshot_batches:
        for value in snapshot_batch[jdbc_multitable_consumer.instance_name].output_lanes.values():
            for record in value:
                processed_data.append(record)
    return processed_data


def delete_table(tables, database):
    for table in tables:
        logger.info('Dropping table %s in %s database...', table.name, database.type)
        table.drop(database.engine)
