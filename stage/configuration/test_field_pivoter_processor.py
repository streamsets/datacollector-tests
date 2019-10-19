import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'copy_all_fields': False}, {'copy_all_fields': True}])
def test_copy_all_fields(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'field_does_not_exist': 'CONTINUE'},
                                              {'field_does_not_exist': 'TO_ERROR'}])
def test_field_does_not_exist(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_field_to_pivot(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'save_original_field_name': True}])
def test_original_field_name_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'copy_all_fields': True}])
def test_pivoted_items_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'copy_all_fields': True,
                                               'pivoted_items_path': 'true',
                                               'remove_pivot_field': False},
                                              {'copy_all_fields': True,
                                               'pivoted_items_path': 'true',
                                               'remove_pivot_field': True}])
def test_remove_pivot_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'save_original_field_name': False}, {'save_original_field_name': True}])
def test_save_original_field_name(sdc_builder, sdc_executor, stage_attributes):
    pass

