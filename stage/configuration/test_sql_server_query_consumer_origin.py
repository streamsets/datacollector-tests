import logging
import string


import pytest
import sqlalchemy
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import category, sdc_min_version
from streamsets.testframework.utils import get_random_string


logger = logging.getLogger(__name__)


pytestmark = [pytest.mark.sdc_min_version('3.15.0'), pytest.mark.database('sqlserver')]


@stub
@category('advanced')
def test_additional_jdbc_configuration_properties(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'auto_commit': False}, {'auto_commit': True}])
def test_auto_commit(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
def test_connection_health_test_query(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
def test_connection_string(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_connection_timeout_in_seconds(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'convert_timestamp_to_string': False},
                                              {'convert_timestamp_to_string': True}])
def test_convert_timestamp_to_string(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'create_jdbc_header_attributes': False},
                                              {'create_jdbc_header_attributes': True}])
def test_create_jdbc_header_attributes(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'disable_query_validation': False}, {'disable_query_validation': True}])
def test_disable_query_validation(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'enable_encryption': False}, {'enable_encryption': True}])
def test_enable_encryption(sdc_builder, sdc_executor, database, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'incremental_mode': False}, {'incremental_mode': True}])
def test_incremental_mode(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
def test_init_query(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
def test_initial_offset(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_jdbc_driver_class_name(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'create_jdbc_header_attributes': True}])
def test_jdbc_header_prefix(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
def test_max_batch_size_in_records(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_max_blob_size_in_bytes(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_max_clob_size_in_characters(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_max_connection_lifetime_in_seconds(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_max_transaction_size(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_maximum_pool_size(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_maximum_transaction_length(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_minimum_idle_connections(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_new_table_discovery_interval(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_no_more_data_event_generation_delay_in_seconds(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_number_of_retries_on_sql_error(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
def test_offset_column(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'on_unknown_type': 'CONVERT_TO_STRING'},
                                              {'on_unknown_type': 'STOP_PIPELINE'}])
def test_on_unknown_type(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_password(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
def test_query_interval(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'root_field_type': 'LIST'},
                                              {'root_field_type': 'LIST_MAP'},
                                              {'root_field_type': 'MAP'}])
def test_root_field_type(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'enable_encryption': True, 'trust_server_certificate': False}])
def test_server_certificate_pem(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
def test_sql_query(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_transaction_id_column_name(sdc_builder, sdc_executor, database):
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
@pytest.mark.parametrize('stage_attributes', [{'enable_encryption': True, 'trust_server_certificate': False},
                                              {'enable_encryption': True, 'trust_server_certificate': True}])
def test_trust_server_certificate(sdc_builder, sdc_executor, database, stage_attributes):
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


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'enable_encryption': True,
                                               'verify_hostname_in_connection_string': False}])
def test_verify_alternate_hostname(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'enable_encryption': True,
                                               'verify_hostname_in_connection_string': False},
                                              {'enable_encryption': True, 'verify_hostname_in_connection_string': True}])
def test_verify_hostname_in_connection_string(sdc_builder, sdc_executor, database, stage_attributes):
    pass

