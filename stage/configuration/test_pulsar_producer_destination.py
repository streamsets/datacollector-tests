import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'async_send': False}, {'async_send': True}])
def test_async_send(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'async_send': True, 'enable_batching': True}])
def test_batch_max_publish_latency_in_ms(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_tls': True}])
def test_ca_certificate_pem(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_mutual_authentication': True, 'enable_tls': True}])
def test_client_certificate_pem(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_mutual_authentication': True, 'enable_tls': True}])
def test_client_key_pem(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'compression_type': 'LZ4'},
                                              {'compression_type': 'NONE'},
                                              {'compression_type': 'ZLIB'}])
def test_compression_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'async_send': True, 'enable_batching': False},
                                              {'async_send': True, 'enable_batching': True}])
def test_enable_batching(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_mutual_authentication': False, 'enable_tls': True},
                                              {'enable_mutual_authentication': True, 'enable_tls': True}])
def test_enable_mutual_authentication(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_tls': False}, {'enable_tls': True}])
def test_enable_tls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'hashing_scheme': 'JAVA_STRING_HASH'},
                                              {'hashing_scheme': 'MUMUR3_32HASH'}])
def test_hashing_scheme(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_keep_alive_interval_in_ms(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'async_send': True, 'enable_batching': True}])
def test_max_batch_size_in_messages(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'async_send': True}])
def test_max_pending_messages(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_message_key(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_operation_timeout_in_ms(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'partition_type': 'ROUND_ROBIN'}, {'partition_type': 'SINGLE'}])
def test_partition_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_pulsar_configuration_properties(sdc_builder, sdc_executor):
    pass


@stub
def test_pulsar_url(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_topic(sdc_builder, sdc_executor):
    pass

