import pytest

from streamsets.testframework.decorators import stub


@stub
def test_fields_to_merge(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'overwrite_fields': False}, {'overwrite_fields': True}])
def test_overwrite_fields(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'source_field_does_not_exist': 'CONTINUE'},
                                              {'source_field_does_not_exist': 'TO_ERROR'}])
def test_source_field_does_not_exist(sdc_builder, sdc_executor, stage_attributes):
    pass

