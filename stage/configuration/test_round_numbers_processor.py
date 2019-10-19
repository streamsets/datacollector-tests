import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'round_method': 'RANGE'}])
def test_range_size(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'round_method': 'ABOVE_BELOW'}, {'round_method': 'RANGE'}])
def test_round_method(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'round_method': 'ABOVE_BELOW'}])
def test_threshold(sdc_builder, sdc_executor, stage_attributes):
    pass

