import pytest


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


@pytest.mark.skip('Not yet implemented')
def test_configuration_required_fields(sdc_builder, sdc_executor):
    pass


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

