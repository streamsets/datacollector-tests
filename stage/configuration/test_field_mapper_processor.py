import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'operate_on': 'FIELD_PATHS'}])
def test_aggregation_expression(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'append_list_values': False, 'operate_on': 'FIELD_NAMES'},
                                              {'append_list_values': True, 'operate_on': 'FIELD_NAMES'},
                                              {'append_list_values': False, 'operate_on': 'FIELD_PATHS'},
                                              {'append_list_values': True, 'operate_on': 'FIELD_PATHS'}])
def test_append_list_values(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_conditional_expression(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'maintain_original_paths': False, 'operate_on': 'FIELD_NAMES'},
                                              {'maintain_original_paths': True, 'operate_on': 'FIELD_NAMES'},
                                              {'maintain_original_paths': False, 'operate_on': 'FIELD_PATHS'},
                                              {'maintain_original_paths': True, 'operate_on': 'FIELD_PATHS'}])
def test_maintain_original_paths(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_mapping_expression(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'operate_on': 'FIELD_NAMES'},
                                              {'operate_on': 'FIELD_PATHS'},
                                              {'operate_on': 'FIELD_VALUES'}])
def test_operate_on(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'operate_on': 'FIELD_NAMES', 'structure_change_allowed': False},
                                              {'operate_on': 'FIELD_NAMES', 'structure_change_allowed': True},
                                              {'operate_on': 'FIELD_PATHS', 'structure_change_allowed': False},
                                              {'operate_on': 'FIELD_PATHS', 'structure_change_allowed': True}])
def test_structure_change_allowed(sdc_builder, sdc_executor, stage_attributes):
    pass

