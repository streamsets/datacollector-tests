import pytest

from streamsets.testframework.decorators import stub


@stub
def test_alert_texts_to_retain(sdc_builder, sdc_executor):
    pass


@stub
def test_auth_token(sdc_builder, sdc_executor):
    pass


@stub
def test_job_id(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_pipeline_configuration(sdc_builder, sdc_executor):
    pass


@stub
def test_pipeline_rules_configuration(sdc_builder, sdc_executor):
    pass


@stub
def test_pipeline_url(sdc_builder, sdc_executor):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_remote_timeseries_url(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_retry_attempts(sdc_builder, sdc_executor):
    pass


@stub
def test_sdc_id(sdc_builder, sdc_executor):
    pass

