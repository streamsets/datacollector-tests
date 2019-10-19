import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'action': 'KEEP'},
                                              {'action': 'REMOVE'},
                                              {'action': 'REMOVE_CONSTANT'},
                                              {'action': 'REMOVE_EMPTY'},
                                              {'action': 'REMOVE_NULL'},
                                              {'action': 'REMOVE_NULL_EMPTY'}])
def test_action(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'action': 'REMOVE_CONSTANT'}])
def test_constant(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass

