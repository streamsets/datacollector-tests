import pytest

from streamsets.testframework.decorators import stub


@stub
def test_batch_wait_time_in_ms(sdc_builder, sdc_executor):
    pass


@stub
def test_connect_timeout_in_ms(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_keepalive_thread': False}, {'enable_keepalive_thread': True}])
def test_enable_keepalive_thread(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_hostname(sdc_builder, sdc_executor):
    pass


@stub
def test_ignore_tables(sdc_builder, sdc_executor):
    pass


@stub
def test_include_tables(sdc_builder, sdc_executor):
    pass


@stub
def test_initial_offset(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_keepalive_thread': True}])
def test_keepalive_interval_in_ms(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_max_batch_size_in_records(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_password(sdc_builder, sdc_executor):
    pass


@stub
def test_port(sdc_builder, sdc_executor):
    pass


@stub
def test_server_id(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'start_from_beginning': False}, {'start_from_beginning': True}])
def test_start_from_beginning(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_ssl': False}, {'use_ssl': True}])
def test_use_ssl(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_username(sdc_builder, sdc_executor):
    pass

