import logging
import string


import pytest
import sqlalchemy
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import category, sdc_min_version
from streamsets.testframework.utils import get_random_string


logger = logging.getLogger(__name__)


pytestmark = [pytest.mark.sdc_min_version('3.15.0'), pytest.mark.database('postgresql')


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


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'convert_timestamp_to_string': False},
                                              {'convert_timestamp_to_string': True}])
def test_convert_timestamp_to_string(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
def test_db_time_zone(sdc_builder, sdc_executor, database):
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

