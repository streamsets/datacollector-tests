import json
import logging

import pytest
import sqlalchemy
from streamsets.testframework.markers import database
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@pytest.mark.skip('Not yet implemented')
def test_configuration_additional_jdbc_configuration_properties(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('change_log_format', ['MSSQL', 'MongoDBOpLog', 'MySQLBinLog', 'NONE', 'OracleCDC'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_change_log_format(sdc_builder, sdc_executor, change_log_format):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_connection_health_test_query(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_connection_timeout_in_seconds(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_data_sqlstate_codes(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('default_operation', ['DELETE', 'INSERT', 'UPDATE'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_default_operation(sdc_builder, sdc_executor, default_operation):
    pass


@pytest.mark.parametrize('enclose_object_names', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_enclose_object_names(sdc_builder, sdc_executor, enclose_object_names):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_field_to_column_mapping(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_idle_timeout_in_seconds(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_init_query(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_jdbc_connection_string(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_jdbc_driver_class_name(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_max_connection_lifetime_in_seconds(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_maximum_pool_size(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_minimum_idle_connections(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('on_record_error', ['DISCARD', 'STOP_PIPELINE', 'TO_ERROR'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_on_record_error(sdc_builder, sdc_executor, on_record_error):
    pass


@pytest.mark.parametrize('use_credentials', [True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_password(sdc_builder, sdc_executor, use_credentials):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_preconditions(sdc_builder, sdc_executor):
    pass


@database
def test_required_fields(sdc_builder, sdc_executor, database, keep_data):
    """Verify that setting a required field sends records without that field to the error stream while records with
    the field are passed through properly.
    """
    DATA = [{'id': 1, 'name': 'Dima'}, {'name': 'Jarcec'}, {'id': 3, 'name': 'Arvind'}]
    EXPECTED_OUTPUT = [(1, 'Dima'), (3, 'Arvind')]

    COLUMNS = [sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, autoincrement=False),
               sqlalchemy.Column('name', sqlalchemy.String(100))]
    # For postgres/oracle database If table's  name are mixed-case then it is required to put quotes in table's name to
    # do any operation.Data collector does not add quotes and hence we are creating table name with lower case letters.
    table_name = get_random_string().lower()
    table = create_table(database, COLUMNS, table_name)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    raw_data = ''.join(json.dumps(record) for record in DATA)
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                           raw_data=raw_data)
    jdbc_producer = pipeline_builder.add_stage('JDBC Producer').set_attributes(default_operation='INSERT',
                                                                               field_to_column_mapping=[],
                                                                               required_fields=['/id'],
                                                                               table_name=table_name)
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    dev_raw_data_source >> [jdbc_producer, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)
    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        with database.engine.connect() as connection:
            result = connection.execute(table.select())
            data_from_database = sorted(result.fetchall(), key=lambda row: row[0])  # order by id

        error_records = snapshot[jdbc_producer].error_records
        assert (data_from_database == EXPECTED_OUTPUT and
                ['COMMON_0001'] == [error_record.header['errorCode']
                                    for error_record in snapshot[jdbc_producer].error_records])
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            delete_table([table], database)


@pytest.mark.parametrize('rollback_batch_on_error', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_rollback_batch_on_error(sdc_builder, sdc_executor, rollback_batch_on_error):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_schema_name(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('use_multi_row_operation', [True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_statement_parameter_limit(sdc_builder, sdc_executor, use_multi_row_operation):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_table_name(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('transaction_isolation', ['DEFAULT', 'TRANSACTION_READ_COMMITTED', 'TRANSACTION_READ_UNCOMMITTED', 'TRANSACTION_REPEATABLE_READ', 'TRANSACTION_SERIALIZABLE'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_transaction_isolation(sdc_builder, sdc_executor, transaction_isolation):
    pass


@pytest.mark.parametrize('unsupported_operation_handling', ['DISCARD', 'SEND_TO_ERROR', 'USE_DEFAULT'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_unsupported_operation_handling(sdc_builder, sdc_executor, unsupported_operation_handling):
    pass


@pytest.mark.parametrize('use_credentials', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_use_credentials(sdc_builder, sdc_executor, use_credentials):
    pass


@pytest.mark.parametrize('use_multi_row_operation', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_use_multi_row_operation(sdc_builder, sdc_executor, use_multi_row_operation):
    pass


@pytest.mark.parametrize('use_credentials', [True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_username(sdc_builder, sdc_executor, use_credentials):
    pass


# utility function
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


def delete_table(tables, database):
    for table in tables:
        logger.info('Dropping table %s in %s database...', table.name, database.type)
        table.drop(database.engine)
