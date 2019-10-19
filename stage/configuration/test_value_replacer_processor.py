import pytest

from streamsets.testframework.decorators import stub


@stub
def test_conditionally_replace_values(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'field_does_not_exist': 'CONTINUE'},
                                              {'field_does_not_exist': 'TO_ERROR'}])
def test_field_does_not_exist(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_fields_to_null(sdc_builder, sdc_executor):
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
def test_replace_null_values(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass

