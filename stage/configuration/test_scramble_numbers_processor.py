import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'allow_negative': False}, {'allow_negative': True}])
def test_allow_negative(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_lower_bound(sdc_builder, sdc_executor):
    pass


@stub
def test_upper_bound(sdc_builder, sdc_executor):
    pass

