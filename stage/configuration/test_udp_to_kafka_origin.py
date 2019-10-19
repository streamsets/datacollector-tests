import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_udp_multithreading': True}])
def test_accept_threads(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_broker_uri(sdc_builder, sdc_executor):
    pass


@stub
def test_charset(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'COLLECTD'},
                                              {'data_format': 'NETFLOW'},
                                              {'data_format': 'SYSLOG'}])
def test_data_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_udp_multithreading': False}, {'enable_udp_multithreading': True}])
def test_enable_udp_multithreading(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_kafka_configuration(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'message_key_format': 'AVRO'}, {'message_key_format': 'STRING'}])
def test_kafka_message_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'key_serializer': 'CONFLUENT', 'message_key_format': 'AVRO'},
                                              {'key_serializer': 'STRING', 'message_key_format': 'AVRO'}])
def test_key_serializer(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'message_key_format': 'AVRO'}, {'message_key_format': 'STRING'}])
def test_message_key_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_missing_field': 'ERROR'}, {'on_missing_field': 'IGNORE'}])
def test_on_missing_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_port(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'pretty_format': False}, {'pretty_format': True}])
def test_pretty_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'quote_mode': 'ALL'}, {'quote_mode': 'MINIMAL'}, {'quote_mode': 'NONE'}])
def test_quote_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{}, {}])
def test_topic(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'validate_schema': False}, {'validate_schema': True}])
def test_validate_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'value_serializer': 'CONFLUENT'}, {'value_serializer': 'DEFAULT'}])
def test_value_serializer(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_write_concurrency(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'validate_schema': True}])
def test_xml_schema(sdc_builder, sdc_executor, stage_attributes):
    pass

