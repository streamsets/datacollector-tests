import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'round_format': 'YYYY'},
                                              {'round_format': 'YYYY_MM'},
                                              {'round_format': 'YYYY_QQ'}])
def test_round_format(sdc_builder, sdc_executor, stage_attributes):
    pass

