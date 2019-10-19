import pytest

from streamsets.testframework.decorators import stub


@stub
def test_batch_size(sdc_builder, sdc_executor):
    pass


@stub
def test_custom_worker_url_list(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'disable_multihead_ingest': False}, {'disable_multihead_ingest': True}])
def test_disable_multihead_ingest(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_ip_regex(sdc_builder, sdc_executor):
    pass


@stub
def test_kinetica_url(sdc_builder, sdc_executor):
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
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_table_name(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'transport_compression': False}, {'transport_compression': True}])
def test_transport_compression(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'update_on_existing_pk': False}, {'update_on_existing_pk': True}])
def test_update_on_existing_pk(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_username(sdc_builder, sdc_executor):
    pass

