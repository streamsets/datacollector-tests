import pytest

from streamsets.testframework.decorators import stub


@stub
def test_application_name(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_ssl': False, 'protocol': 'TCP'},
                                              {'enable_ssl': True, 'protocol': 'TCP'}])
def test_enable_ssl(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_hostname(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'message_format': 'RFC_3164', 'protocol': 'UDP'},
                                              {'message_format': 'RFC_5424', 'protocol': 'UDP'},
                                              {'message_format': 'RFC_5425', 'protocol': 'UDP'}])
def test_message_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_message_id(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_process_id(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'protocol': 'TCP'}, {'protocol': 'UDP'}])
def test_protocol(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_severity_level(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'protocol': 'TCP'}])
def test_socket_connection_timeout_in_ms(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_syslog_facility(sdc_builder, sdc_executor):
    pass


@stub
def test_syslog_host(sdc_builder, sdc_executor):
    pass


@stub
def test_syslog_port(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'protocol': 'TCP'}])
def test_tcp_connection_retries(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_timestamp(sdc_builder, sdc_executor):
    pass

