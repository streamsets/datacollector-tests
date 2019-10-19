import pytest

from streamsets.testframework.decorators import stub


@stub
def test_batch_wait_time_in_ms(sdc_builder, sdc_executor):
    pass


@stub
def test_max_batch_size_in_reports(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'POLLING'}])
def test_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'POLLING'}])
def test_omniture_report_description(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_omniture_rest_url(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_proxy_uri(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'POLLING'}])
def test_report_request_interval_in_ms(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_request_timeout(sdc_builder, sdc_executor):
    pass


@stub
def test_shared_secret(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': False}, {'use_proxy': True}])
def test_use_proxy(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}, {}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass

