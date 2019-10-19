import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'compare': 'ALL_FIELDS'}, {'compare': 'SPECIFIED_FIELDS'}])
def test_compare(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'compare': 'SPECIFIED_FIELDS'}])
def test_fields_to_compare(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_max_records_to_compare(sdc_builder, sdc_executor):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_time_to_compare_in_secs(sdc_builder, sdc_executor):
    pass

