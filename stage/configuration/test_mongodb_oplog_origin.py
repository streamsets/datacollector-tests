import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'USER_PASS'}])
def test_authentication_source(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'LDAP'},
                                              {'authentication_type': 'NONE'},
                                              {'authentication_type': 'USER_PASS'}])
def test_authentication_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_batch_size_in_records(sdc_builder, sdc_executor):
    pass


@stub
def test_collection(sdc_builder, sdc_executor):
    pass


@stub
def test_connect_timeout(sdc_builder, sdc_executor):
    pass


@stub
def test_connection_string(sdc_builder, sdc_executor):
    pass


@stub
def test_connections_per_host(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'cursor_finalizer_enabled': False}, {'cursor_finalizer_enabled': True}])
def test_cursor_finalizer_enabled(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_single_mode': False}, {'enable_single_mode': True}])
def test_enable_single_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_heartbeat_connect_timeout(sdc_builder, sdc_executor):
    pass


@stub
def test_heartbeat_frequency(sdc_builder, sdc_executor):
    pass


@stub
def test_heartbeat_socket_timeout(sdc_builder, sdc_executor):
    pass


@stub
def test_initial_ordinal(sdc_builder, sdc_executor):
    pass


@stub
def test_initial_timestamp_in_secs(sdc_builder, sdc_executor):
    pass


@stub
def test_local_threshold(sdc_builder, sdc_executor):
    pass


@stub
def test_max_batch_wait_time(sdc_builder, sdc_executor):
    pass


@stub
def test_max_connection_idle_time(sdc_builder, sdc_executor):
    pass


@stub
def test_max_connection_life_time(sdc_builder, sdc_executor):
    pass


@stub
def test_max_wait_time(sdc_builder, sdc_executor):
    pass


@stub
def test_min_connections_per_host(sdc_builder, sdc_executor):
    pass


@stub
def test_min_heartbeat_frequency(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_operation_types(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'LDAP'}, {'authentication_type': 'USER_PASS'}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'read_preference': 'NEAREST'},
                                              {'read_preference': 'PRIMARY'},
                                              {'read_preference': 'PRIMARY_PREFERRED'},
                                              {'read_preference': 'SECONDARY'},
                                              {'read_preference': 'SECONDARY_PREFERRED'}])
def test_read_preference(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_replica_set_name(sdc_builder, sdc_executor):
    pass


@stub
def test_server_selection_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'socket_keep_alive': False}, {'socket_keep_alive': True}])
def test_socket_keep_alive(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_socket_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ssl_enabled': False}, {'ssl_enabled': True}])
def test_ssl_enabled(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ssl_invalid_host_name_allowed': False},
                                              {'ssl_invalid_host_name_allowed': True}])
def test_ssl_invalid_host_name_allowed(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_threads_allowed_to_block_for_connection_multiplier(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'LDAP'}, {'authentication_type': 'USER_PASS'}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass

