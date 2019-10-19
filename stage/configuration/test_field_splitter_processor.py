import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'too_many_splits': 'TO_LIST'}])
def test_field_for_remaining_splits(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_field_to_split(sdc_builder, sdc_executor):
    pass


@stub
def test_new_split_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'not_enough_splits': 'CONTINUE'}, {'not_enough_splits': 'TO_ERROR'}])
def test_not_enough_splits(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'original_field': 'KEEP'}, {'original_field': 'REMOVE'}])
def test_original_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_separator(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'too_many_splits': 'TO_LAST_FIELD'}, {'too_many_splits': 'TO_LIST'}])
def test_too_many_splits(sdc_builder, sdc_executor, stage_attributes):
    pass

