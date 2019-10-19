import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'transformation': 'ABBREVIATE'}, {'transformation': 'FIRSTONLY'}])
def test_transformation(sdc_builder, sdc_executor, stage_attributes):
    pass

