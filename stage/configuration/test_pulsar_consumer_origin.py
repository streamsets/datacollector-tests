import pytest

from streamsets.testframework.decorators import stub


@stub
def test_batch_wait_time_in_ms(sdc_builder, sdc_executor):
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
def test_consumer_name(sdc_builder, sdc_executor):
    pass


@stub
def test_consumer_priority_level(sdc_builder, sdc_executor):
    pass


@stub
def test_consumer_queue_size(sdc_builder, sdc_executor):
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
@pytest.mark.parametrize('stage_attributes', [{'initial_offset': 'EARLIEST'}, {'initial_offset': 'LATEST'}])
def test_initial_offset(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_keep_alive_interval_in_ms(sdc_builder, sdc_executor):
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
def test_operation_timeout_in_ms(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'topics_selector': 'TOPICS_PATTERN'}])
def test_pattern_auto_discovery_period_in_minutes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'produce_single_record': False}, {'produce_single_record': True}])
def test_produce_single_record(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_pulsar_configuration_properties(sdc_builder, sdc_executor):
    pass


@stub
def test_pulsar_url(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'read_compacted': False, 'subscription_type': 'EXCLUSIVE'},
                                              {'read_compacted': True, 'subscription_type': 'EXCLUSIVE'},
                                              {'read_compacted': False, 'subscription_type': 'FAILOVER'},
                                              {'read_compacted': True, 'subscription_type': 'FAILOVER'}])
def test_read_compacted(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_subscription_name(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'subscription_type': 'EXCLUSIVE'},
                                              {'subscription_type': 'FAILOVER'},
                                              {'subscription_type': 'SHARED'}])
def test_subscription_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'topics_selector': 'SINGLE_TOPIC'}])
def test_topic(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'topics_selector': 'TOPICS_LIST'}])
def test_topics_list(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'topics_selector': 'TOPICS_PATTERN'}])
def test_topics_pattern(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'topics_selector': 'SINGLE_TOPIC'},
                                              {'topics_selector': 'TOPICS_LIST'},
                                              {'topics_selector': 'TOPICS_PATTERN'}])
def test_topics_selector(sdc_builder, sdc_executor, stage_attributes):
    pass

