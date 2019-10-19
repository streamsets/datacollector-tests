import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'missing_mask_behavior': 'APPLY_PARTIAL_MASK'}])
def test_mask_for_partial_match(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_masks(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'missing_mask_behavior': 'APPLY_PARTIAL_MASK'},
                                              {'missing_mask_behavior': 'DROP_FIELD'},
                                              {'missing_mask_behavior': 'TERMINATE_EXECUTION'}])
def test_missing_mask_behavior(sdc_builder, sdc_executor, stage_attributes):
    pass

